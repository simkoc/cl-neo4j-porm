(defpackage :de.tu-braunschweig.cs.ias.cl-neo4j-porm
  (:nicknames :cl-neo4j-porm)
  (:use :cl :cl-user :drakma :cl-ppcre :cl-json :trivial-utf-8)
  (:export ;; entity
   get-type
   ;; value entity
   value
   get-value ;deprecated
   ;; graph entity
   id
   entity=
   get-property
   get-attribute ;deprecated
   get-id ;deprecated
   ;; node
   forget-graph-position
   node
   label
   has-label-p
   relationships<-
   relationships<-*
   relationships->
   relationships->*
   ;; relationship
   relationship
   start
   end
   rel-type
   ;; graph
   node
   nodes
   relationship
   relationships<-
   ;; neo4j-interaction
   with-neo4j-connection
   simple-query
   graph-query
   results
   graphs))
