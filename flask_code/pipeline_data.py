class PipelineData:
    # This class is used to store Dataframe thats read from Parquet Files.
   _instance = None



   def __new__(cls):
       if not cls._instance:
           cls._instance = super().__new__(cls)
           cls._instance.data = {}
       return cls._instance


   def set_data(self, key, value):
       self.data[key] = value


   def get_data(self, key):
       return self.data.get(key)
  
   def clear_data(self, key):
       self.data.pop(key, None)
