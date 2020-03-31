def splitDate(date):
	day = int(date.split('T')[0].split('-')[2])
	month = int(date.split('T')[0].split('-')[1])
	year = int(date.split('T')[0].split('-')[0])
	return year,month,day

def formDate(year,month,day,time):
	if month < 10:
		month = '0'+str(month)
	if day < 10:
		day = '0'+str(day)
	date = '-'.join([str(year),str(month),str(day)])
	return date+"T"+time

def getDuration(start,end):
	startTime = start.split('T')[1].split('.')[0].split(':')
	endTime = end.split('T')[1].split('.')[0].split(':')
	return int(endTime[0])*3600+int(endTime[1])*60+int(endTime[2])-int(startTime[0])*3600-int(startTime[1])*60-int(startTime[2])