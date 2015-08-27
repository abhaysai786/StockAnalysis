A = load 'hdfs:///pigdata' USING PigStorage(',','-tagFile');

B = FILTER A BY $1 != 'Date'; 

C = FOREACH B GENERATE $0,FLATTEN(STRSPLIT($1,'-',3)) AS (YEAR:int,MONTH:int,DAY:int),(double)$7;

D = GROUP C BY ($0,$1,$2);

E =  FOREACH D {
                 M = ORDER C BY DAY ASC;
                 N = ORDER C BY DAY DESC;                 
                 MINM = LIMIT M 1;
                 MAXN = LIMIT N 1;
                 GENERATE FLATTEN(MINM),FLATTEN(MAXN);
};

H = FOREACH E GENERATE $0,$1,$2,($9-$4)/$4;

I = group H by $0;
J = FOREACH I GENERATE $0,FLATTEN(AVG(H.$3)) as avg;


K= join H BY ($0) , J BY ($0);

M = FOREACH K GENERATE $0 , ($3-$5)*($3-$5);

N = GROUP M BY $0;

OT = FOREACH N GENERATE $0,FLATTEN(SQRT(SUM(M.$1)/(COUNT(M)-1)));

O = FILTER OT BY $1 != 0.0;

P = ORDER O BY $1 ASC;
DESCRIBE P;
Q = LIMIT P 10;
DESCRIBE Q;


R = ORDER O BY $1 DESC;
DESCRIBE R;
S = LIMIT R 10;
DESCRIBE S;


RESULT = UNION Q,S;
DUMP RESULT;

store RESULT into 'hdfs:///pigdata/hw3_out';

