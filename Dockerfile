FROM python3.7
WORKDIR /app
COPY swissre.py swissre.py
COPY smallacc.log smallacc.log
CMD ["python3","./swissre.py","-input","smallacc.log","-output","EventsPerSec","-outputtype","csv"]