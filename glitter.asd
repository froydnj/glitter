; -*- mode: lisp -*-

(asdf:defsystem :glitter
  :version "0.1"
  :author "Nathan Froyd <froydnj@gmail.com>"
  :maintainer "Nathan Froyd <froydnj@gmail.com>"
  :description "A Common Lisp library for accessing git repositories"
  :depends-on (:binascii)
  :components ((:static-file "README")
               (:static-file "LICENSE")
               (:file "tree")))
