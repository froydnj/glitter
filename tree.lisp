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
             ((<= +ascii-zero+ byte +ascii-nine+)
              (* base (- byte +ascii-zero+)))
             ((<= +ascii-a+ byte +ascii-z+)
              (* base (+ 10 (- byte +ascii-a+))))
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

(defvar *versioned-packfile-signature* #())

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
  (let ((contents (make-array (file-length stream)
                              :element-type '(unsigned-byte 8))))
    (read-sequence contents stream)
    (setf (contents index) contents)
    index))

(defmethod shared-initialize :after ((index pack-index-v1) slots &rest initargs
                                     &key filename stream)
  (setf (fanout-table index) (read-fanout-table (contents index) 0))
  index)

(defmethod shared-initialize :after ((index pack-index-v2) slots &rest initargs
                                     &key filename stream)
  (let* ((fanout-table (read-fanout-table (contents index) 8))
         (name-table-offset (+ 8 +fanout-table-size+))
         (crc32-table-offset (+ name-table-offset (* 20 +fanout-table-n-entries+)))
         (pack-offset-table-offset (+ crc32-table-offset (* 4 n-fanout-entries))))
    (setf (fanout-table index) fanout-table
          (name-table-offset index) name-table-offset
          (crc32-table-offset index) crc32-table-offset
          (pack-offset-table-offset index) pack-offset-table-offset)
    pack))

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
     do (let ((offset (index-name-offset index i))
              (comparison (compare-shas contents offset sha 0)))
          (cond
            ((< comparison 0)
             (setf start (1+ i)))
            ((> comparison 0)
             (setf start (1- i)))
            (t
             (return-from find-sha-in-index i))))
     finally (return nil)))
