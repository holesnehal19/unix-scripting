Scripting

# Problem Statement : 
You have multiple files with differnet tenant, doamin and table on server A.Script is on server B. you have to download all files for specific tenant from server A to server B. After downloading all files on server B you have to store it into hive table to do further analysis.
file_name eg : tenant_domain_tablename_currentday(yyyymmdd)_previousday(yyyymmddtime)_instanceName.txt

Solution :
1. Download the files from server A to server B
2. check for domain and table name. If it get matched then Retrive date from that file name.
3. load file into hive table but we cant load file into partitioned table so first we need to load it into internal temporary    hive table and then insert overwrite data into exteranl partitioned table.
4. drop internal table.
5. move file from server A /out to Server A /archive
6. move file from server B /out to Server B / archive/currentday


- Once you load the data into table then you can generate any report or you can do analysis.

