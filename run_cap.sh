# set up
CONFIG_FILE='main/CohortAnalysisPipeline/config.json'

removeFolderIfExist(){
	if [ -d $1 ] 
	then 
		rm -r $1
		echo "removed $1"
	fi
}

terminateIfNotExist(){
	if [ ! -d $1 ]
	then
		echo "$1 not found"
		exit
	fi
}

# Stage 1: ShiftFactorExtractor
echo '================= RUNNING STAGE 1: SHIFTFACTOREXTRACTOR ================='

SFE_SCRIPT='main/CohortAnalysisPipeline/ShiftFactorExtractor/ShiftFactorExtractor.py'
SFE_INPUT='main/data/device_logs_new' # DIRECTORY OF DEVICE LOGS
SFE_OUTPUT='main/data/intermediate_output/CAP_01'
echo 'Checking if $SFE_OUTPUT exists ...'
removeFolderIfExist $SFE_OUTPUT
terminateIfNotExist $SFE_INPUT
spark-submit --master local[*] $SFE_SCRIPT -i $SFE_INPUT -o $SFE_OUTPUT -c $CONFIG_FILE

# Stage 2: EventTransformer
echo '================= RUNNING STAGE 2: EVENTTRANSFORMER ================='

ET_SCRIPT='main/CohortAnalysisPipeline/EventTransformer/EventTransformer.py'
ET_INPUT='main/data/intermediate_output/CAP_01'
ET_OUTPUT='main/data/intermediate_output/CAP_02'
echo 'Checking if $ET_OUTPUT exists ...'
removeFolderIfExist $ET_OUTPUT
terminateIfNotExist $ET_INPUT
spark-submit --master local[*] $ET_SCRIPT -i $ET_INPUT -o $ET_OUTPUT

# Stage 3: EventClassifier
echo '================= RUNNING STAGE 3: EVENTCLASSIFIER ================='

EC_SCRIPT='main/CohortAnalysisPipeline/EventClassifier/EventClassifier.py'
EC_INPUT='main/data/intermediate_output/CAP_02' 
EC_OUTPUT='main/data/intermediate_output/CAP_03'
echo 'Checking if $EC_OUTPUT exists ...'
removeFolderIfExist $EC_OUTPUT
terminateIfNotExist $EC_INPUT
spark-submit --master local[*] $EC_SCRIPT -i $EC_INPUT -o $EC_OUTPUT -c $CONFIG_FILE

# Stage 4: UsageCounter
echo '================= RUNNING STAGE 4: USAGECOUNTER ================='

UC_SCRIPT='main/CohortAnalysisPipeline/UsageCounter/UsageCounter.py'
UC_INPUT='main/data/intermediate_output/CAP_03' 
UC_MAP='main/data/reference/content_mapping.csv'
UC_OUTPUT='main/data/intermediate_output/CAP_04'
echo 'Checking if $UC_OUTPUT exists ...'
removeFolderIfExist $UC_OUTPUT
terminateIfNotExist $UC_INPUT
spark-submit --master local[*] $UC_SCRIPT -i $UC_INPUT -m $UC_MAP -o $UC_OUTPUT -c $CONFIG_FILE

# Stage 5: UsageAggregator
echo '================= RUNNING STAGE 5: USERAGGREGATOR ================='

UA_SCRIPT='main/CohortAnalysisPipeline/UserAggregator/UserAggregator.py'
UA_INPUT='main/data/intermediate_output/CAP_04' 
UA_MAP='main/data/reference/profile_mapping.csv'
UA_OUTPUT='main/data/final_output/CAP_05'
echo 'Checking if $UA_OUTPUT exists ...'
removeFolderIfExist $UA_OUTPUT
terminateIfNotExist $UA_INPUT
spark-submit --master local[*] $UA_SCRIPT -i $UA_INPUT -m $UA_MAP -o $UA_OUTPUT -c $CONFIG_FILE

