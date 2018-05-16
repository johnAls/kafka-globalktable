# kafka-globalktable
Test when restoring a gktable never finish, the tests is in src/test/java/se/mm/GlobalKTableIntegrationTest.java

The problem seems that the offset and highwater mark differ in GlobalStateManagerImpl
