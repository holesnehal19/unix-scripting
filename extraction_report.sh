#!/bin/bash
#set -e
#----------------Make sure to have KC3P_HOME is set----------------#

if [[ -z $KC3P_HOME ]];
then
    echo "KC3P_HOME path is not set"
    exit 1;
fi;

#----------------Import common functions----------------#

. $KC3P_HOME/bin/kc3p-common.sh

script_file=$(basename $0)

#----------------Create log directory----------------#

log_dir=$KC3P_CONF/uuid_extraction/logs

mkdir -p $log_dir

#----------------Create error file----------------#

error_logfile=$log_dir/${script_file}_$(date '+%Y%m%d%H%M%S').error


#---------------Set HRZ archive path-------------------#

hrz_archive_path=/x/home/pp_adm/batch-eds-monarch/prod/uuid/archive/out

#----------------Script Parameter Name Validation----------------#

options=$(getopt -o dserq -l "env:,tenant:,scp_host:,scp_path:" "--"  "$@")
f_write_log "Command executed : $0 $options" $logfile

#----------------Setup Parameter Value----------------#
eval set -- "$options"
while true; do
    case "$1" in
        --env) shift; env="$1"
        ;;
        --tenant) shift; tenant="$1"
        ;;
        --scp_host) shift; scp_host="$1"
        ;;
        --scp_path) shift; scp_path="$1"
        ;;
        --)
          shift
          break
        ;;
    esac
shift
done

#----------------Script Parameter Value Validation----------------#

if [[ -z $env ]] || [[ -z $tenant ]] || [[ -z $scp_host ]] || [[ -z $scp_path ]];
then
    f_write_log "Incorrect Parameters Provided" $error_logfile
    f_write_log "Expected Usage Is : $0 --env <<env_name>> --tenant <<tenant_name>> --scp_host <<scp_host>> --scp_path <<scp_path>>" $error_logfile
    f_send_email "UUID Extraction Report Execution Script Failed" "shole@paypal.com" "$error_logfile"
    exit 1;
fi;
#----------------Import domain and tables property file----------------#

. $KC3P_CONF/uuid_extraction/${tenant}/venmo_uuid_extraction_report.properties

#----------------Create log file----------------#

logfile=${log_dir}/${tenant}_${script_file}_$(date '+%Y%m%d%H%M%S').log
f_write_log "Log file path: $logfile" $logfile

#-----------------set uuid report hql path--------------------#

hiveql_path=$KC3P_CONF/uuid_extraction/common/hql/uuid_report.hql
f_write_log "HQL path: $hiveql_path" $logfile

#----------------Setting output File Location----------------#

extracted_uuid_path_paz="/x/home/pp_batch_tahoe_prod/${tenant}/uuid_extraction/out"

#----------------Extract Year Month Day and time----------------#
previous_day=`TZ=aaa24 date +%Y%m%d`
current_day=`date +%Y%m%d`
current_time=`date +%s`
f_write_log " previous_day = $previous_day" $logfile
f_write_log " current_day = $current_day" $logfile
f_write_log " current_time = $current_time" $logfile

#----------------Create KC3P UUID Extraction Directory----------------#

mkdir -p ${extracted_uuid_path_paz}/${current_day}
f_write_log "HRZ to PAZ downloaded files path: ${extracted_uuid_path_paz}/${current_day}" $logfile

#----------------Download Files from  HRZ to PAZ----------------#

f_write_log " **********UUID Extraction Report Script Execution Started**********\n" $logfile

cd ${extracted_uuid_path_paz}/${current_day}/
sftp pp_adm@${scp_host} <<EOT
get ${scp_path}/*${tenant}*.txt
quit
EOT
if [ $? -ne 0 ];
then
      f_write_log "HRZ to PAZ download command failed" $logfile
      f_send_email "UUID_Extraction_Report Execution Script Failed" "shole@paypal.com" "$logfile"
      exit 1;
fi

#----------------Retrieve file list----------------------#

file_list=`ls $extracted_uuid_path_paz/${current_day}/`
#----------------Retrieve file list----------------------#

domain_list=`echo $domain | sed 's/:/ /g'`
f_write_log "Domain list: $domain_list" $logfile
#----------------Retrieve file list----------------------#
table_list=`echo $tables | sed 's/:/ /g'`
f_write_log "Table list: $table_list" $logfile

f_write_log "***************Data load Started**************\n" $logfile

for domain_name in $domain_list
do
  f_write_log "Data load started for domain: $domain_name" $logfile
  for file_name in $file_list
  do
                                                                                                                                                                                                                                                              131,3         60%
    if [[ $file_name == *"$domain_name"* ]];
    then
      for table_name in $table_list
        do
          if [[ $file_name == *"$table_name"* ]];
          then
            partition_date=`echo $file_name | rev | cut -d'_' -f4 | rev`
            year=${partition_date:0:4}
            month=${partition_date:4:2}
            day=${partition_date:6:2}
            sed -i 1d $extracted_uuid_path_paz/${current_day}/$file_name
            file_path="$extracted_uuid_path_paz/${current_day}/$file_name"
            hive --hiveconf hive.root.logger=OFF --hiveconf year=$year --hiveconf month=$month --hiveconf day=$day --hiveconf current_time=$current_time --hiveconf file_path="$file_path" --hiveconf table_name=$table_name --hiveconf tenant=$tenant --hiveconf domain_name=$domain_name -f $hiveql_path
            if [ $? -ne 0 ];
            then
                f_write_log " Data load command failed: hive --hiveconf hive.root.logger=OFF --hiveconf year=$year --hiveconf month=$month --hiveconf day=$day --hiveconf current_time=$current_time --hiveconf file_path=$file_path --hiveconf table_name=$table_name --hiveconf tenant=$tenant --hiveconf domain_name=$domain_name -f $hiveql_path" $logfile
                f_send_email "Data load command failed" "shole@paypal.com" "$logfile"
                exit 1;
            fi
          fi
      done
    fi
  done
done


f_write_log "***************Data loaded successfully**************\n" $logfile

#--------------------Move UUID out file into archive for HRZ--------------#

ssh -n pp_adm@${scp_host} mv ${scp_path}/*${tenant}* ${hrz_archive_path}/

f_write_log "Moved files from HRZ out to archive\n" $logfile

#--------------------Move UUID out file into Archive for PAZ---------------#
extracted_uuid_archive_path="/x/home/pp_batch_tahoe_prod/${tenant}/uuid_extraction/archive"
mkdir -p $extracted_uuid_archive_path/${current_day}

mv $extracted_uuid_path_paz/${current_day}/* $extracted_uuid_archive_path/${current_day}
f_write_log "uuid report files moved from $extracted_uuid_path_paz/${current_day}/ to $extracted_uuid_archive_path/${current_day}" $logfile

rm -rf $extracted_uuid_archive_path/${previous_day}
f_write_log "Deleted uuid report files for previous day: $extracted_uuid_archive_path/${previous_day} " $logfile
