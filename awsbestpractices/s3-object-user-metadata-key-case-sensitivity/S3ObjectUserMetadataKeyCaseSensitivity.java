package example;

public class Example {
    
    ObjectMetadata objMetaData;

    void uppercaseAtCall1() {
        objMetaData.getUserMetaDataOf("Key");
    }

    void lowercaseAtCall1() {
        objMetaData.getUserMetaDataOf("key");
    }

    void uppercaseAtCall2() {
        objMetaData.getUserMetadata().get("Key");
    }

    void lowercaseAtCall2() {
        objMetaData.getUserMetadata().get("key");
    }

    void uppercaseFromVariable1() {
        String var = "Key";
        objMetaData.getUserMetaDataOf(var);
    }

    void lowercaseFromVariable1() {
        String var = "key";
        objMetaData.getUserMetaDataOf(var);
    }

    void uppercaseFromVariable2() {
        String var = "Key";
        objMetaData.getUserMetadata().get(var);
    }

    void lowercaseFromVariable2() {
        String var = "key";
        objMetaData.getUserMetadata().get(var);
    }

    void uppercaseFromConcat1() {
        String var = v + "Key";
        objMetaData.getUserMetaDataOf(var);
    }

    void lowercaseFromConcat1() {
        String var = "a" + "key";
        objMetaData.getUserMetaDataOf(var);
    }

    void uppercaseFromConcat2() {
        String var = "a" + "Key";
        objMetaData.getUserMetadata().get(var);
    }

    void lowercaseFromConcat2() {
        String var = v + "key";
        objMetaData.getUserMetadata().get(var);
    }

    void lowercaseFromToLowerCase() {
        String var = "a" + "Key".toLowerCase();
        objMetaData.getUserMetadata().get(var);
    }

    void uppercaseFromToLowerCase() {
        String var = v.toLowerCase() + "Key";
        objMetaData.getUserMetadata().get(var);
    }

    void lowercaseFromToUpperCase() {
        String var = "a" + "Key".toUpperCase().toLowerCase();
        objMetaData.getUserMetadata().get(var);
    }

    void uppercaseFromToUpperCase() {
        String var = v.toUpperCase() + "key";
        objMetaData.getUserMetadata().get(var);
    }

}