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
