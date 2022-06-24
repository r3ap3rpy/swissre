import argparse, re, os, logging, json
from logging.handlers import RotatingFileHandler
from collections import defaultdict
from datetime import datetime, timezone
from unittest import result
xtrawhtspc = re.compile(r'\s+')
ipv4pat = re.compile("\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")

class IPCounter:
    def __init__(self, data):
        self._counts = defaultdict(int)
        self._data = data
    def __call__(self, argument):
        self._counts[argument] += 1
        return self._counts[argument]
    
    def process(self):
        for line in self._data:
            if line:
                for character in line:
                    if character:
                        self.__call__(character)

    def __repr__(self):
        return str(sorted(self._counts.items(), key=lambda kv: kv[1]))




CWD = os.path.sep.join(os.path.abspath(__file__).split(os.path.sep)[:-1])
LOGS = os.path.sep.join([CWD, 'Logs'])
LOGFILENAME = f'SwissRe.log'

if not os.path.isdir(LOGS):
    os.mkdir(LOGS)

parser = argparse.ArgumentParser(description='Homework for the SwissRe company interview process!')
parser.add_argument('-input', type = str, nargs = '+', help='Path to one or more plain text files, or a directory', required=True)
parser.add_argument('-inputext', type = str, default='log', help='Extension of the files to be used, default is *.log*', required=False)
parser.add_argument('-output', type = str, choices=['MostFreqIP', 'LeastFreqIP', 'EventsPerSec','TotalBytesExch'], required=True)
parser.add_argument('-outputpath', type = str, help="The path to the output to be produced!", default='.', required=False)
parser.add_argument('-outputtype', type = str, help="The type of the output file to be produced!", choices=("csv","json"), default="txt" ,required=False)

args = parser.parse_args()

logging.basicConfig(format='%(asctime)s %(levelname)s :: %(message)s', level=logging.INFO)
logger = logging.getLogger('SwissRe')
handler = RotatingFileHandler(os.path.sep.join([LOGS,LOGFILENAME]), mode='a', maxBytes=1000000, backupCount=100, encoding='utf-8', delay=0)
handler.setLevel(logging.INFO)
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)

logger.info("####### Start #######")
logger.info(f"# Specified input: {','.join(args.input)}")
logger.info(f"# Specified input ext: {args.inputext}")
logger.info(f"# Specified output: {args.output}")
logger.info(f"# Specified output path: {args.outputpath}")
logger.info(f"# Specified output type: {args.outputtype}")

Results = {}
FilesToProcess = []
logger.info(f"## Preprocessing input files!")
for f in args.input:
    if os.path.isfile(f) and f.endswith(f'.{args.inputext}'):
        FilesToProcess.append(f)
    if os.path.isdir(f):
        for file in os.listdir(f):
            if file.endswith(f'.{args.inputext}'):
                FilesToProcess.append(os.path.sep.join([f,file]))

if FilesToProcess:
    logger.info(f"# The list of files which will be processed: {','.join(FilesToProcess)}")
    for file in FilesToProcess:
        logger.info(f"# Working on file: {file}")
        with open(file,'r',encoding='utf-8') as f:
            data =[ re.sub(' +',' ',_.strip()) for _ in f.readlines() if _.strip()]
        if data:
            if args.output == 'TotalBytesExch':
                try:
                    TBXCH = sum([int(_[4]) for _ in [ _ for _ in data]])
                except Exception as e:
                    TBXCH = str(e)
                Results.update({file: {args.output : TBXCH}})
            if args.output in ['MostFreqIP','LeastFreqIP']:
                UniqueIPs = set(re.findall(ipv4pat, os.linesep.join(data)))
                Freq = {}
                for ip in UniqueIPs:
                    Freq.update({ip:len(re.findall(ip, os.linesep.join(data)))})
                Freq = {k: v for k, v in sorted(Freq.items(), key=lambda item: item[1])}
                
                if args.output == 'MostFreqIP':
                    MF = [ _ for _ in Freq.items()][-1]
                    Results.update({file:{args.output : {MF[0]:MF[1]}}})
                else:
                    LF = [ _ for _ in Freq.items()][0]
                    Results.update({file:{args.output : {LF[0]:LF[1]}}})
            if args.output == 'EventsPerSec':
                tmp = []
                StampDict = {}
                for line in data:
                    newline = [str(datetime.fromtimestamp(int(eval(line.split(' ')[0])),timezone.utc)).split('+')[0]]
                    newline.extend(line.split(' ')[1:-1])
                    tmp.append(newline)
                UniqueStamps = set([ _[0] for _ in tmp])
                for stamp in UniqueStamps:
                    StampDict[stamp] = len([ _ for _ in tmp if _[0] == stamp])
                Results.update({file:{args.output:(round(sum(StampDict.values()) / len(StampDict),2))}})
            print(Results)
            if Results:
                outputfile = os.path.sep.join([args.outputpath,f'result.{args.outputtype}'])
                logger.info(f"Dumping results as {args.outputtype} to {outputfile}")
                if args.outputtype == 'json':
                    with open(outputfile,'w',encoding='utf-8') as ofile:
                        json.dump(dict(Results),ofile,ensure_ascii=False, indent=4)
                if args.outputtype == 'csv':
                    Columns = ['File','Output','Value']
                    with open(outputfile,'w') as f:
                        f.write(','.join(Columns) + os.linesep)
                        for file in Results:
                            for item in Results[file]:
                                f.write(','.join([file,item,str(Results[file][item])]) + os.linesep)
            else:
                logger.critical("There are no results to be saved to the output file!")
        else:
            logger.critical(f"There is no data in the file, skipping: {file}")

else:
    logger.critical(f"There are no files to be processed!")

logger.info("######## End ########")

