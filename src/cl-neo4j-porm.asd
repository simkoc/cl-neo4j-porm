(asdf:defsystem :cl-neo4j-porm
  :name "cl-neo4j"
  :author "Simon Koch <simon.koch@tu-braunschweig.de>"
  :maintainer "Simon Koch <simon.koch@tu-braunschweig.de>"
  :depends-on (:drakma
               :cl-ppcre
               :cl-json
               :trivial-utf-8)
  :components ((:file "package")
               (:file "request"
                :depends-on ("package"))))
