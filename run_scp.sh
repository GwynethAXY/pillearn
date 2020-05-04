# set up
CONFIG_FILE='main/SubscriptionContentPipeline/config.ini'

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

# Stage 1: SchemaTransformer
echo '================= RUNNING STAGE 1: SCHEMATRANSFORMER ================='

ST_SCRIPT='main/SubscriptionContentPipeline/SchemaTransformer/SchemaTransformer.py'
ST_INPUT='main/data/device_logs_new' # DIRECTORY OF DEVICE LOGS
ST_OUTPUT='main/data/intermediate_output/SCP_01'
echo 'Checking if $ST_OUTPUT exists ...'
removeFolderIfExist $ST_OUTPUT
terminateIfNotExist $ST_INPUT
spark-submit --master local[*] $ST_SCRIPT -i $ST_INPUT -o $ST_OUTPUT -c $CONFIG_FILE

# Stage 2: PopularityCalculator
echo '================= RUNNING STAGE 2: POPULARITYCALCULATOR ================='

PC_SCRIPT='main/SubscriptionContentPipeline/PopularityCalculator/PopularityCalculator.py'
PC_INPUT='main/data/intermediate_output/SCP_01'
PC_OUTPUT='main/data/intermediate_output/SCP_02'
PC_CONTENT='main/data/reference/content_mapping.parquet'
echo 'Checking if $PC_OUTPUT exists ...'
removeFolderIfExist $PC_OUTPUT
terminateIfNotExist $PC_INPUT
spark-submit --master local[*] $PC_SCRIPT -i $PC_INPUT -o $PC_OUTPUT -c $CONFIG_FILE -cm $PC_CONTENT

# Stage 3: PopularityAggregator
echo '================= RUNNING STAGE 3: POPULARITYAGGREGATOR ================='

PA_SCRIPT='main/SubscriptionContentPipeline/PopularityAggregator/PopularityAggregator.py'
PA_INPUT='main/data/intermediate_output/SCP_02'
PA_MAP='main/data/reference/profile_mapping.csv'
PA_OUTPUT='main/data/final_output/SCP_03'
echo 'Checking if $PA_OUTPUT exists ...'
removeFolderIfExist $PA_OUTPUT
terminateIfNotExist $PA_INPUT
spark-submit --master local[*] $PA_SCRIPT -i $PA_INPUT -m $PA_MAP -o $PA_OUTPUT -c $CONFIG_FILE
