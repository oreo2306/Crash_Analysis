import yaml
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col,sum,count,desc,row_number,rank,split,broadcast,max

spark = SparkSession.builder \
    .appName("CrashAnalysis") \
    .master("local[*]")\
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.hadoop.io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g")  \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


class CrashAnalysis:
    def __init__(self, charges_df ,damages_df ,endorse_df ,primary_person_df ,restrict_df ,units_df, output_path):
        self.charges_df = charges_df
        self.damages_df = damages_df
        self.endorse_df = endorse_df
        self.primary_person_df = primary_person_df
        self.injury_cases_df = primary_person_df.filter(col("PRSN_INJRY_SEV_ID").isin("KILLED", "INCAPACITATING INJURY", "NON-INCAPACITATING INJURY", "POSSIBLE INJURY"))
        self.restrict_df = restrict_df
        self.units_df = units_df
        self.units_car_df = units_df.filter(col("VEH_BODY_STYL_ID").like("%CAR%"))
        self.output_path = output_path        

    def analysis1(self): # Find the number of crashes where the number of males killed are greater than 2.
        result_df_1 = self.primary_person_df.filter((col("PRSN_GNDR_ID") == "MALE") & (col("DEATH_CNT")  == 1 ))
        result_df_2 = result_df_1.groupBy("CRASH_ID").agg(sum("DEATH_CNT").alias("total_male_deaths"))
        count = result_df_2.filter(col("total_male_deaths") > 2).count()
        
        print(f"Analysis 1 - Crashes with more than 2 males killed: {count}")
    
    def analysis2(self): # Count how many two-wheelers are booked for crashes.
        count = self.units_df.filter(col("VEH_BODY_STYL_ID").like("%MOTORCYCLE%")).count()
        print(f"Analysis 2 - Number of two-wheelers booked for crashes: {count}")
   
    def analysis3(self): # Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
        driver_killed_df = self.injury_cases_df.filter(
            (col("PRSN_TYPE_ID") == "DRIVER") & 
            (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED") & 
            (col("PRSN_INJRY_SEV_ID") == "KILLED"))
        result_df = driver_killed_df.join(self.units_car_df, on=["CRASH_ID", "UNIT_NBR"], how="inner").groupBy("VEH_MAKE_ID").agg(count("*").alias("crash_count")).orderBy(desc("crash_count")).limit(5)
        print("Analysis 3 - Top 5 Vehicle Makes:")
        result_df.show()

    def analysis4(self): # Determine number of Vehicles with driver having valid licences involved in hit and run.
        joined_df = self.units_df.alias("un").join(
            broadcast(self.endorse_df).alias("en"), on=["CRASH_ID", "UNIT_NBR"],how="left").join(
            broadcast(self.restrict_df).alias("res"), on=["CRASH_ID", "UNIT_NBR"], how="left")
            # Assuming a license is considered valid if:
            # - It has at least one endorsement (DRVR_LIC_ENDORS_ID is not null)
            # - It has no restrictions (DRVR_LIC_RESTRIC_ID is null)
        valid_license_hit_and_run_count = joined_df.filter(
            (col("en.DRVR_LIC_ENDORS_ID") !="NONE") &  # Has endorsements
            (col("res.DRVR_LIC_RESTRIC_ID") =="NONE") &   # No restrictions
            (col("un.VEH_HNR_FL") == "Y")).count()             # Hit-and-run case
        print(f"Analysis 4 - Vehicles with valid licences involved in hit and run: {valid_license_hit_and_run_count}")

    def analysis5(self): # Which state has highest number of accidents in which females are not involved?
        result_df = self.primary_person_df.filter(col("PRSN_GNDR_ID") != "FEMALE").join(self.units_df, on=["CRASH_ID", "UNIT_NBR"]).groupBy("VEH_LIC_STATE_ID").agg(count("*").alias("accident_count"))
        max_accident_count = result_df.agg(max("accident_count").alias("max_count")).collect()[0]["max_count"]
        top_state_df = result_df.filter(col("accident_count") == max_accident_count)
        print(f"Analysis 5 - State with the highest number of accidents without females involved:")
        top_state_df.show()

    def analysis6(self): # Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries including death?
        result_df = self.injury_cases_df.join(self.units_df, on=["CRASH_ID", "UNIT_NBR"], how="inner").groupBy("VEH_MAKE_ID").agg(count("*").alias("injury_count"))
        result_with_row_numbers = result_df.withColumn("row_number", row_number().over(Window.orderBy(desc("injury_count"))))
        top_3_to_5_vehicle_makes_df = result_with_row_numbers.filter((col("row_number") >= 3) & (col("row_number") <= 5)) \
        .select("VEH_MAKE_ID")
        print(f"Analysis 6 - Top 3rd to 5th VEH_MAKE_IDs contributing to the largest number of injuries including death:")
        top_3_to_5_vehicle_makes_df.show()
        
    def analysis7(self): # For all the body styles involved in crashes, mention the top ethnic user group of each unique body style.
        result_df = self.primary_person_df.join(self.units_df, on="CRASH_ID").groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").agg(count("*").alias("ethnicity_count"))
        result_df = result_df.withColumn("rank", rank().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(desc("ethnicity_count"))))
        top_ethnic_group_by_body_style = result_df.filter(col("rank") == 1).select("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID","ethnicity_count")
        print(f"Analysis 7 - Top ethnic user group of each unique body style:")
        top_ethnic_group_by_body_style.show(100,False)

    def analysis8(self): # Among the crashed cars, what are the Top 5 Zip Codes with the highest number of crashes with alcohol as the contributing factor to a crash?
        units_car_zip_df = self.units_car_df.filter(col("OWNR_ZIP") !="NULL")
        alcohol_related_df = self.primary_person_df.filter(col("PRSN_ALC_RSLT_ID")=="Positive")
        result_df_join = alcohol_related_df.join(units_car_zip_df, on=["CRASH_ID", "UNIT_NBR"] )
        result_df_distinct=result_df_join.select("CRASH_ID","OWNR_ZIP").distinct()
        result_df=result_df_distinct.groupBy("OWNR_ZIP").agg(count("CRASH_ID").alias("crash_count")).orderBy(desc("crash_count")).limit(5)
        print("Analysis 8 - Top 5 Zip Codes:")
        result_df.show()

    def analysis9(self): # Count of Distinct Crash IDs where No Damaged Property was observed, Damage Level (VEH_DMAG_SCL~) is above 4, and car avails Insurance.
        damages_property_df = self.damages_df.filter(
            (col("DAMAGED_PROPERTY").like("NONE%")) | 
            (col("DAMAGED_PROPERTY").isNull()))
        units_damage_df = self.units_df.withColumn("damage_level", split(col("VEH_DMAG_SCL_1_ID"), " ")[1].cast("int"))
        units_damage_df = units_damage_df.filter((col("damage_level")>4) & (col("FIN_RESP_PROOF_ID") !='NA')).select("CRASH_ID")
        result_count = damages_property_df.join(units_damage_df, on="CRASH_ID").select("CRASH_ID").distinct().count()
        print(f"Analysis 9 - Count of Distinct Crash IDs with no damaged property and damage level above 4: {result_count}")

    def analysis10(self): # Determine the Top 5 Vehicle Makes where drivers are charged with speeding-related offences, have licensed Drivers, used top 10 used vehicle colours,and have cars licensed in the Top 25 states with the highest number of offences.
       
        speeding_charges_df = self.charges_df.filter(col("CHARGE").like("%SPEED%"))
        licensed_drivers_df = self.endorse_df.filter(col("DRVR_LIC_ENDORS_ID") !="NONE")

        top_10_colors_df = self.units_df.groupBy("VEH_COLOR_ID").agg(count("*").alias("color_count")).orderBy(desc("color_count")).limit(10)
        top_colors = [row["VEH_COLOR_ID"] for row in top_10_colors_df.collect()]  # Collect top color IDs
        vehicles_with_top_colors_df = self.units_df.filter(col("VEH_COLOR_ID").isin(top_colors))

        top_25_states_df = self.units_df.groupBy("VEH_LIC_STATE_ID").agg(count("*").alias("state_count")).orderBy(desc("state_count")).limit(25)
        top_states = [row["VEH_LIC_STATE_ID"] for row in top_25_states_df.collect()]  # Collect top state IDs
        vehicles_in_top_color_and_states_df = vehicles_with_top_colors_df.filter(col("VEH_LIC_STATE_ID").isin(top_states))

        filtered_units_df = vehicles_in_top_color_and_states_df.join(
            speeding_charges_df, on=["CRASH_ID", "UNIT_NBR"]).join(
            licensed_drivers_df, on=["CRASH_ID", "UNIT_NBR"])

        result_df = filtered_units_df.groupBy("VEH_MAKE_ID").agg(count("CRASH_ID").alias("offense_count")).orderBy(desc("offense_count")).limit(5)
        print("Analysis 10 - Top 5 Vehicle Makes:")
        result_df.show()
        
def load_config(config_path="config/config.yaml"):
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

if __name__ == "__main__":
    # Load configuration
    config = load_config()

    # Load datasets based on config paths
    primary_person_df = spark.read.csv(config["data_paths"]["primary_person"], header=True, inferSchema=True)
    units_df = spark.read.csv(config["data_paths"]["units"], header=True, inferSchema=True)
    charges_df = spark.read.csv(config["data_paths"]["charges"], header=True, inferSchema=True)
    endorsements_df = spark.read.csv(config["data_paths"]["endorse"], header=True, inferSchema=True)
    restrict_df = spark.read.csv(config["data_paths"]["restrict"], header=True, inferSchema=True)
    damages_df = spark.read.csv(config["data_paths"]["damages"], header=True, inferSchema=True)

    # Create a object for analysis class and pass input path from config
    analysis = CrashAnalysis(charges_df ,damages_df ,endorsements_df ,primary_person_df ,restrict_df ,units_df, config["output_path"])

    # Run analyses
    analysis.analysis1()
    analysis.analysis2()
    analysis.analysis3()
    analysis.analysis4()
    analysis.analysis5()
    analysis.analysis6()
    analysis.analysis7()
    analysis.analysis8()
    analysis.analysis9()
    analysis.analysis10()