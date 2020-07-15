package concurrency;

import amazon.odin.awsauth.OdinSymmetricKey;

import java.util.concurrent.ConcurrentHashMap;

public class GlobalOdinSymmetricKeyProvider {
  private static volatile GlobalOdinSymmetricKeyProvider globalOdinSymmetricKeyProvider;
  private static ConcurrentHashMap<String,OdinSymmetricKey> materialSetTable = new ConcurrentHashMap<String,OdinSymmetricKey>();
    private GlobalOdinSymmetricKeyProvider(){}
    
    /* This class is singleton as we want tp have a single object of OdinSymmetricKey
     * which will refresh itself in 10 mins.
     */    
    public static GlobalOdinSymmetricKeyProvider getInstance(){
        if(GlobalOdinSymmetricKeyProvider.globalOdinSymmetricKeyProvider==null){
          synchronized(GlobalOdinCredentialsProvider.class){
            if(GlobalOdinSymmetricKeyProvider.globalOdinSymmetricKeyProvider==null){
              globalOdinSymmetricKeyProvider = new GlobalOdinSymmetricKeyProvider();
            }
      }
        }
        return GlobalOdinSymmetricKeyProvider.globalOdinSymmetricKeyProvider;
    }
    
    /* Gets the Odin Symmetric Key Provider stored in the Concurrent Hash Map
     * OdinSymmetricKey is a long lived object which refreshes itself after every 10 mins
     * ConcurrentHashMap is thread safe Map where reads are more than updates.
     */
    public OdinSymmetricKey getOdinSymmetricKeyProvider(String materialSet) throws Exception{
      if(!materialSetTable.containsKey(materialSet)){
          materialSetTable.putIfAbsent(materialSet, new OdinSymmetricKey(materialSet));
      }
      return materialSetTable.get(materialSet);
    }
}