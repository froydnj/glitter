(in-package :cl-user)

(defclass actor ()
  ((name :accessor name :initarg :name)
   (email :accessor email :initarg :email)
   (timestamp :accessor timestamp :initarg :timestamp)
   (timezone :accessor timezone :initarg :timezone)))

(defclass commit ()
  ((tree-hash :accessor tree-hash :initarg :tree-hash)
   (parent-hashes :accessor parent-hashes :initarg :parent-hashes :type list)
   (author :accessor author :initarg :author)
   (committer :accessor committer :initarg :committer)
   (message :accessor message :initarg :message)))

(defclass tree ()
  ((entries :reader entries :initform (make-hash-table :test #'equalp))))

(defclass tree-entry ()
  ((mode :reader mode :initarg :mode)
   (name :reader name :initarg :name)
   (sha1 :reader sha1 :initarg :sha1)))

(defclass blob ()
  ((content :reader content :initarg :content)))

(defun read-number-from-buffer (buffer &key (start 0) end (radix 10))
  (declare (type (simple-array (unsigned-byte 8) (*)) buffer))
  (declare (type (integer 2 36) radix))
  (loop with end = (or end (length buffer))
     for i from (1- end) downto start
     for base = 1 then (* base radix)
     sum (let ((byte (aref buffer i)))
           (cond
             ((typep byte '(integer #x30 #x39))
              (* base (- byte #x30)))
             ((typep byte '(integer #x61 #x7a))
              (* base (+ 10 (- byte #x61))))
             (t (error "Invalid byte: ~A in ~A"
                       byte (subseq buffer start end)))))))

(defun git-parse-tree-object (buffer)
  (let* ((null-pos (position 0 buffer))
         (space-pos (position (char-code #\Space) buffer))
         (size (archive::read-number-from-buffer buffer :start (1+ space-pos) :end null-pos)))
    (flet ((read-tree-entry (start)
             (let* ((space-pos (position (char-code #\Space) buffer :start start))
                    (null-pos (position 0 buffer :start start))
                    (entry-end (+ null-pos 21)))
               (values (make-instance 'tree-entry
                                      :mode (read-number-from-buffer buffer :start start :end space-pos)
                                      :name (sb-ext:octets-to-string buffer :external-format :utf-8 :start (1+ space-pos) :end null-pos)
                                      :sha1 (subseq buffer (1+ null-pos) entry-end))
                       entry-end))))
      (loop with length = (length buffer)
         with tree = (make-instance 'tree)
         with start = (1+ null-pos)
         while (< start length)
         do (multiple-value-bind (entry new-start) (read-tree-entry start)
              (setf (gethash (name entry) (entries tree)) entry
                    start new-start))
         finally (return (values size tree))))))

(defun git-parse-blob-object (buffer)
  (let ((null-pos (position 0 buffer)))
    (make-instance 'blob :content (subseq buffer (1+ null-pos)))))

(defparameter *parse-objects-methods*
  (list (cons (sb-ext:string-to-octets "blob") 'git-parse-blob-object)
        (cons (sb-ext:string-to-octets "commit") 'git-parse-commit-object)
        (cons (sb-ext:string-to-octets "tree") 'git-parse-tree-object)))

(defun git-parse-object (buffer)
  (loop for (id . parser) in *parse-objects-methods*
     unless (mismatch id buffer :end2 (length id))
     do (return-from git-parse-object (funcall parser buffer))
     finally (error "don't know how to parse ~A" buffer)))

(defun read-octet-vec (stream n-octets)
  (let ((vec (make-array n-octets :element-type '(unsigned-byte 8))))
    (read-sequence vec stream)
    vec))

(defvar *versioned-index-signature* #(#xff #x74 #x4f #x63))

(defclass pack-index ()
  ((contents :accessor contents)
   (fanout-table :accessor fanout-table)))

(defclass pack-index-v1 (pack-index)
  ())

(defclass pack-index-v2 (pack-index)
  ((name-table-offset :accessor name-table-offset)
   (crc32-table-offset :accessor crc32-table-offset)
   (pack-offset-table-offset :accessor pack-offset-table-offset)))

(defun load-pack-index (filename)
  (with-open-file (stream filename
                          :element-type '(unsigned-byte 8)
                          :direction :input)
    (let ((signature (read-octet-vec stream 4)))
      (cond
        ((not (mismatch signature *versioned-packfile-signature*))
         (let ((version (ironclad:ub32ref/be (read-octet-vec stream 4) 0)))
           (case version
             (2
              (file-position stream 0)
              (make-instance 'pack-index-v2 :filename filename
                             :stream stream))
             (t
              (error "Unknown pack index format ~D" version)))))
        (t
         (file-position stream 0)
         (make-instance 'pack-index-v1 :filename filename
                        :stream stream))))))

(defconstant +fanout-table-n-entries+ 256)
(defconstant +fanout-table-entry-size+ 4)
(defconstant +fanout-table-size+ (* +fanout-table-n-entries+
                                    +fanout-table-entry-size+))

(defun read-fanout-table (buffer start)
  (loop with table = (make-array +fanout-table-n-entries+)
     for i from 0 below +fanout-table-n-entries+
     for offset from start by +fanout-table-entry-size+
     do (setf (aref table i) (ironclad:ub32ref/be buffer offset))
     finally (return table)))

(defmethod shared-initialize :after ((index pack-index) slots &rest initargs
                                     &key filename stream)
  (declare (ignore initargs filename))
  (let ((contents (make-array (file-length stream)
                              :element-type '(unsigned-byte 8))))
    (read-sequence contents stream)
    (setf (contents index) contents)
    index))

;;; A v1 index file has the following structure:
;;;
;;; - A "fanout" table: a 256-entry array of 32-bit big-endian integers.
;;;   The easiest way to describe the fanout table is to describe the
;;;   in-memory data structure can be used to construct:
;;;
;;;   [00] -> [...object IDs...] ; A
;;;   [01] -> NULL
;;;   [02] -> [...object IDs...] ; B
;;;   ...
;;;   [ff] -> [...object IDs...] ; C
;;;
;;;   In this table, the entries are sorted lists of object IDs that
;;;   begin with the index of the slot they occupy.  The list A has
;;;   object IDs that begin with the byte #x00, the list B has object
;;;   IDs that begin with the byte #x02, and so forth.
;;;
;;;   How do you get there from the fanout table?  The fanout table
;;;   tracks the cumulative total of object IDs.  So the fanout table
;;;   looks like:
;;;
;;;   [00] -> (length A)
;;;   [01] -> (length A) ; no object IDs beginning with #x01
;;;   [02] -> (+ (length A) (length B))
;;;   ...
;;;   [ff] -> # of object IDs in packfile
;;;
;;;   This is actually not quite right: the lists in the first diagram
;;;   are (offset, object ID), where OFFSET is a 32-bit big-endian
;;;   integer that represents an offset into the packfile where the
;;;   object can be found.
;;;
;;; - (offset, object ID) tuples as described above;
;;;
;;; - A SHA1 checksum.
(defmethod shared-initialize :after ((index pack-index-v1) slots &rest initargs
                                     &key filename stream)
  (declare (ignore initargs filename stream))
  (setf (fanout-table index) (read-fanout-table (contents index) 0))
  index)

;;; A v2 index file shares the same underlying structure as a v1 index
;;; file, but adds a few useful details.  From the top:
;;;
;;; - A four-byte magic ID number: *VERSIONED-PACKFILE-SIGNATURE*;
;;;
;;; - A 32-bit, big-endian version identifier.  The only version that's
;;;   currently used is 2;
;;;
;;; - A fanout table, as described above.  The concept of counting remains
;;;   the same, but the per-entry data is split up slightly differently;
;;;
;;; - An object ID table, sorted.  There are (AREF FANOUT-TABLE #xff)
;;;   entries in this table;
;;;
;;; - A CRC32 table.  There are (AREF FANOUT-TABLE #xff) entries in this
;;;   table.  As you might imagine, the entries in this table correspond
;;;   to the entries in the object ID table.
;;;
;;; - An offset table; offsets are 32-bit, big-endian integers.  Same
;;;   number of entries as the CRC32 table, same correspondence to the
;;;   object ID table.  Any entries with their most significant bit
;;;   (MSB) set indicate a 64-bit offset into the packfile.  The
;;;   remainder of the bits are looked up in a separate table.
;;;
;;; - A 64-bit offset table; only present if any entries in the offset
;;;   table had their MSB set.  Note that this table has as many entries
;;;   as the number of offset entries with their MSB set, *not* the same
;;;   number of entries as the earlier tables.
;;;
;;; - A SHA1 checksum.
(defmethod shared-initialize :after ((index pack-index-v2) slots &rest initargs
                                     &key filename stream)
  (declare (ignore initargs filename stream))
  (let* ((fanout-table (read-fanout-table (contents index) 8))
         (name-table-offset (+ 8 +fanout-table-size+))
         (crc32-table-offset (+ name-table-offset
                                (* 20 +fanout-table-n-entries+)))
         (pack-offset-table-offset (+ crc32-table-offset
                                      (* 4 +fanout-table-n-entries+))))
    (setf (fanout-table index) fanout-table
          (name-table-offset index) name-table-offset
          (crc32-table-offset index) crc32-table-offset
          (pack-offset-table-offset index) pack-offset-table-offset)
    index))

(defclass index-entry ()
  ((name :initarg :name :reader name)
   (offset :initarg :offset :reader offset)
   (crc32 :initform nil :reader crc32)))

(defgeneric index-name-offset (index i)
  (:method ((index pack-index-v1) i)
    (+ +fanout-table-size+ (* i 24) 4))
  (:method ((index pack-index-v2) i)
    (+ (name-table-offset index) (* i 20))))

(defgeneric index-entry (index i)
  (:method ((index pack-index-v1) i)
    (let* ((offset-start (+ +fanout-table-size+ (* i 24)))
           (name-start (+ offset-start 4))
           (contents (contents index)))
      (make-instance 'index-entry
                     :name (subseq contents name-start (+ name-start 20))
                     :offset (ironclad:ub32ref/be contents offset-start))))
  (:method ((index pack-index-v2) i)
    (let ((contents (contents index))
          (name-start (+ (name-table-offset index) (* i 20)))
          (offset-start (+ (pack-offset-table-offset index) (* i 4)))
          (crc32-start (+ (crc32-table-offset index) (* i 4))))
      (make-instance 'index-entry
                     :name (subseq contents name-start (+ name-start 20))
                     :offset (ironclad:ub32ref/be contents offset-start)
                     :crc32 (ironclad:ub32ref/be contents crc32-start)))))

(defun compare-shas (h1 start1 h2 start2)
  (loop for i from 0 below 20
     for index1 from start1
     for index2 from start2
     do (let ((d (- (aref h1 index1) (aref h2 index2))))
          (unless (zerop d)
            (return-from compare-shas d)))
     finally (return 0)))

(defun find-sha-in-index (index sha start end)
  (loop with contents = (contents index)
     while (<= start end)
     for i = (truncate (+ start end) 2)
     do (let* ((offset (index-name-offset index i))
               (comparison (compare-shas contents offset sha 0)))
          (cond
            ((< comparison 0)
             (setf start (1+ i)))
            ((> comparison 0)
             (setf start (1- i)))
            (t
             (return-from find-sha-in-index i))))
     finally (return nil)))
