(in-package :de.tu-braunschweig.cs.ias.cl-neo4j-porm)


;; parsing utility


(defun create-hash-table (element-list)
  (let ((table (make-hash-table :test 'equalp)))
    (mapcar #'(lambda (elem)
                (setf (gethash (if (numberp (car elem))
                                   (car elem)
                                   (string-downcase (format nil "~a" (car elem))))
                               table)
                          (cdr elem)))
            element-list)
    table))


;; class definitions


(defclass entity () ())


(defgeneric get-entity-type (entity)
  (:documentation "returns the type of the entity"))


(defgeneric print-object (entity stream)
  (:documentation "print the entity object"))


(defgeneric get-attribute (attribute-name entity)
  (:documentation "get the attribute attribute-name from the entity"))


(defmethod get-attribute (attribute-name (entity entity))
  (gethash attribute-name (slot-value entity 'attribute-table)))


(defmethod print-object ((entity entity) stream)
  (format stream "(entity)"))


(defclass value (entity)
  ((value :initarg :value
          :reader value)))


(defun make-value-entity (json-column json-meta)
  (make-instance 'value
                 :attribute-table (if (listp json-column)
                                      (create-hash-table json-column)
                                      (create-hash-table (list (cons 0 json-column))))
                 :id (cdar json-meta)))


(defmethod get-entity-type ((entity value))
  :value)


(defclass graph-entity (entity)
  ((properties :initarg :properties
               :reader properties)
   (id :initarg :id
       :reader id)))


(defclass node (graph-entity)
  ((label :initarg :label
          :initform nil
          :reader label)
   (relationships<- :initarg :relationships<-
                     :initform (list)
                     :reader relationships<-)
   (relationships-> :initarg :relationships->
                      :initform (list)
                      :reader relationships->)))


(defmethod print-object ((entity node) stream)
  (with-slots (id label properties)
      entity
    (let ((properties-list (list)))
      (maphash #'(lambda (key value)
                   (push (list key value) properties-list))
               properties)
      (let ((string (format nil "(:~a {~{~{~a:~a~}~^, ~}})" label properties-list)))
        (format stream string)))))


(defun make-node-entity-using-meta (json-column json-meta)
  (make-instance 'node
                 :label ""
                 :properties (if (listp json-column)
                                 (create-hash-table json-column)
                                 (create-hash-table (list (cons 0 json-column))))
                 :id (parse-integer (cdar json-meta))))


(defun make-node-entity (json-column)
  (destructuring-bind (id labels properties)
      json-column
    (assert (listp (cdr properties)))
    (make-instance 'node
                   :id (parse-integer (cdr id))
                   :label (cdr labels)
                   :properties (create-hash-table (cdr properties)))))


(defmethod get-entity-type ((entity node))
  :node)


(defclass relationship (graph-entity)
  ((start :initarg :start
          :reader start)
   (end :initarg :end
        :reader end)
   (rel-type :initarg :rel-type
             :reader rel-type)))


(defmethod print-object ((entity relationship) stream)
  (with-slots (id rel-type properties start end)
      entity
    (let ((properties-list (list)))
      (maphash #'(lambda (key value)
                   (push (list key value) properties-list))
               properties)
      (if (and (typep start 'node)
               (typep end 'node))
          (let ((string (format nil "(~a)-[:~a {~{~{~a:~a~}~^, ~}}]-(~a)" (id start) rel-type properties-list (id end))))
            (format stream string))
          (let ((string (format nil "[:~a {~{~{~a:~a~}~^, ~}}]" rel-type properties-list)))
            (format stream string))))))


(defun make-relationship-entity-using-meta (json-column json-meta)
  (make-instance 'relationship
                 :properties (if (listp json-column)
                                 (create-hash-table json-column)
                                 (create-hash-table (list (cons 0 json-column))))
                 :rel-type ""
                 :start -1
                 :end -1
                 :id (cdar json-meta)))


(defun make-relationship-entity (json-column)
  (destructuring-bind (id type start-node end-node properties)
      json-column
    (assert (listp (cdr properties)))
    (make-instance 'relationship
                   :id (parse-integer (cdr id))
                   :rel-type (cdr type)
                   :start (parse-integer (cdr start-node))
                   :end (parse-integer (cdr end-node))
                   :properties (create-hash-table (cdr properties)))))


(defmethod get-type ((entity relationship))
  :relationship)


(defclass graph ()
  ((nodes :initarg :nodes
          :reader nodes)
   (relationships :initarg :relationships
                  :reader relationships)
   (node-index :initform (make-hash-table))
   (relationship-index :initform (make-hash-table))))


(defmethod initialize-instance :after ((graph graph) &rest stuff)
  (declare (ignore stuff))
  (with-slots (nodes relationships node-index relationship-index)
      graph
    (format t "~a~%~a~%" nodes relationships)
    (mapcar (lambda (node)
              (assert (integerp (id node)))
              (setf (gethash (id node) node-index) node))
            nodes)
    (mapcar (lambda (relationship)
              (with-slots (id start end)
                  relationship
                (assert (and (integerp start) (integerp end)))
                (setf (gethash id relationship-index) relationship)
                (push relationship (slot-value (gethash start node-index) 'relationships->))
                (setf start (gethash start node-index))
                (push relationship (slot-value (gethash end node-index) 'relationships<-))
                (setf end (gethash end node-index))))
            relationships)))


(defmethod print-object ((graph graph) stream)
  (with-slots (nodes relationships)
      graph
    (format stream "<Graph~%~{~a~%~}~{~a~^~%~}>"
            nodes relationships)))
