// COMMANDs from spark shell ----------


val ubsDataStartOfDay = spark
  .read
  .format("csv")
  .option("inferSchema", "true")
  .option("header", "true")
  .load("/data/assigment1-data/csv/Input_StartOfDay_Positions.txt")

/* UDF starts
  for input startofday positions of particular scrip(2 entries per scrip there) do below custom calculation
  while iterating through ubsTransSeparateJsonObj dataframe*/
def calculateExpectedEODPosn(): = 

	/*custom logic i.e. If Transaction Type =B ,
									For AccountType=E, Quantity=Quantity + TransactionQuantity
									For AccountType=I, Quantity=Quantity - TransactionQuantity
	If Transaction Type =S ,
									For AccountType=E, Quantity=Quantity - TransactionQuantity
									For AccountType=I, Quantity=Quantity + TransactionQuantity
	UDF would return dataframe	of net volume data 	*/

	val ubsTransSeparateJsonObj = spark.read.format("json").option("inferSchema", "true").load("/data/assigment1-data/csv/1537277231233_Input_Transactions_separate_json.txt")//spark expects each line to be a separate json array.usually done using some tool.

	//create 2 dataframes:one to write final Expected_EndOdDay_Postions.txt and one to hold net volume data
	ubsExpectedEODPosnDataframe.write.format("csv")
	.option("mode", "OVERWRITE")
	.option("path", "/data/assigment1-data/csv/Expected_EndOfDay_Positions.txt")
	.save()

	return ubsNetVoumeDataframe;//calculated based on net volume logic
//UDF ends

ubsDataStartOfDay.calculateExpectedEODPosn()//call UDF

ubsNetVoumeDataframe.select("Instrument",min("NetVoume"))//lowest net transaction volumes 
ubsNetVoumeDataframe.select("Instrument",max("NetVoume"))//largest net transaction volumes 