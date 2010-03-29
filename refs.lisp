(in-package :glitter)

(defclass packed-ref ()
  ((hash :initarg :hash :reader ref-hash)
   (name :initarg :name :reader ref-name)))

(defparameter *print-packed-refs-sha-length* 6)

(defmethod print-object ((object packed-ref) stream)
  (print-unreadable-object (object stream)
    (format stream "Packed-Ref ~A ~A" (ref-name object)
            (binascii:encode (ref-hash object) :hex
                             :end *print-packed-refs-sha-length*))))

(defun read-packed-refs-file (filename)
  (with-open-file (stream filename :direction :input
                          :element-type 'character)
    (do ((line (read-line stream nil stream)
           (read-line stream nil stream))
         (refs nil))
        ((eq line stream) (nreverse refs))
      (cond
        ((char= (aref line 0) #\#)
         ;; FIXME: parse '# pack-refs with:' lines
         )
        ((char= (aref line 0) #\^)
         ;; FIXME: parse these
         )
        (t
         (let* ((hash-end (position #\Space line))
                (hash (binascii:decode line :hex :end hash-end))
                (name (subseq line (1+ hash-end))))
           (push (make-instance 'packed-ref :hash hash :name name)
                 refs)))))))
