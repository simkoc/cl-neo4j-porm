(asdf:defsystem :cl-neo4j-porm
  :name "cl-neo4j"
  :author "Simon Koch <simon.koch@tu-braunschweig.de>"
  :version "2.0"
  :maintainer "Simon Koch <simon.koch@tu-braunschweig.de>"
  :depends-on (:drakma
               :cl-ppcre
               :cl-json
               :trivial-utf-8)
  :components ((:file "package")
               (:file "connection"
                      :depends-on ("package"))
               (:file "entities"
                      :depends-on ("package"))
               (:file "result"
                      :depends-on ("package"
                                   "entities"))
               (:file "request"
                      :depends-on ("package"
                                   "connection"
                                   "result"))))
