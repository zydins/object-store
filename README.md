# Append-only object store implementation

This is implementation of append-only object store based of file system. This library is also allows removing of elements.

Objects that are inserted into the repository **must** implement the _Seriazlizable_ interface. All required information is stored in files, index with quick links from _guid_ to the file and position there is built on the run. 

More information about implementation can be found in Java Doc.

Example of using you can find in **ObjectStoreExample.java**. Quick illustration:
```
ObjectStore store = new FileSystemObjectStore(path);
String guid = store.put("String is Seriazlizable");
Object saved = store.get(guid);
store.delete(guid);
```

### How to build executable jar with test:
```
 mvn -Dmaven.test.skip=true package
```
