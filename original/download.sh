# Spider
if ! (test -d spider)
then
    echo "************************ DOWNLOADING DATASET: Spider ************************"
    mkdir spider
    wget https://huggingface.co/datasets/spider/resolve/main/data/spider.zip
    unzip spider.zip
    rm spider.zip
else
    echo "Spider is already downloaded."
fi

# SParC
if ! (test -d SParC)
then
    echo "************************ DOWNLOADING DATASET: SParC ************************"
    echo "!!WARNING!! please manually download the dataset here: https://drive.google.com/uc?export=download&id=1Uu7NMHTR1tdQw1t7bAuM7OPU4LElVKfg"
    echo "!!WARNING!! once download is complete, then type: unzip sparc.zip"
else
    echo "SParC is already downloaded."
fi

# CoSQL
if ! (test -d CoSQL)
then
    echo "************************ DOWNLOADING DATASET: CoSQL ************************"
    echo "!!WARNING!! please manually download the dataset here: https://drive.google.com/uc?export=download&id=1Y3ydpFiQQ3FC0bzdfy3groV95O_f1nXF"
    echo "!!WARNING!! once download is complete, then type: unzip cosql_dataset.zip"
else
    echo "CoSQL is already downloaded."
fi

# WikiSQL
if ! (test -d WikiSQL)
then
    echo "************************ DOWNLOADING DATASET: WikiSQL ************************"
    mkdir wikisql
    wget https://github.com/jkkummerfeld/text2sql-data/blob/master/data/wikisql.json.bz2?raw=true
    mv wikisql.json.bz2?raw=true wikisql/wikisql.json.bz2

    bzip2 -d wikisql/wikisql.json.bz2

    # TODO: Get the schemas
    wget https://github.com/salesforce/WikiSQL/raw/master/data.tar.bz2
    mv data.tar.bz2 wikisql/data.tar.bz2
    tar xvjf wikisql/data.tar.bz2

    mv data/* wikisql/
    rm -rf data
    rm wikisql/data.tar.bz2
else
    echo "WikiSQL is already downloaded."
fi

# KaggleDBQA
if ! (test -d kaggledbqa)
then
    echo "************************ DOWNLOADING DATASET: KaggleDBQA ************************"
    echo "Please go to official KaggleDBQA github and download data manually: https://github.com/Chia-Hsuan-Lee/KaggleDBQA"
else
    echo "KaggleDBQA is already downloaded."
fi

# FIBEN
if ! (test -d fiben-benchmark)
then
    echo "************************ DOWNLOADING DATASET: FIBEN ************************"
    git clone https://github.com/IBM/fiben-benchmark.git
    unzip fiben-benchmark/data.zip -d fiben-benchmark
    rm fiben-benchmark/data.zip
else
    echo "FIBEN is already downloaded."
fi

# ParaphraseBench
if ! (test -d ParaphraseBench)
then
    echo "************************ DOWNLOADING DATASET: ParaphraseBench ************************"
    git clone https://github.com/DataManagementLab/ParaphraseBench.git

else
    echo "ParaphraseBench is already downloaded."
fi

# Criteria2SQL
if ! (test -d Criteria2SQL)
then
    echo "************************ DOWNLOADING DATASET: Criteria2SQL ************************"
    git clone https://github.com/xiaojingyu92/Criteria2SQL.git

else
    echo "Criteria2SQL is already downloaded."
fi

# SEOSS-Queries
if ! (test -d SEOSS-Queries)
then
    echo "************************ DOWNLOADING DATASET: SEOSS-Queries ************************"
    echo "Dataset source link: https://figshare.com/s/75ed49ef01ac2f83b3e2"

    mkdir SEOSS-Queries
    wget https://figshare.com/ndownloader/files/31898543
    mv 31898543 SEOSS-Queries/

    unzip SEOSS-Queries/31898543 -d SEOSS-Queries
    rm SEOSS-Queries/31898543
else
    echo "SEOSS-Queries is already downloaded."
fi

# ACL-SQL
if ! (test -d sql-nlp)
then
    echo "************************ DOWNLOADING DATASET: ACL-SQL ************************"
    git clone https://github.com/rohitshantarampatil/sql-nlp.git
else
    echo "ACL-SQL is already downloaded."
fi

# xsp
if ! (test -d xsp)
then
    echo "************************ DOWNLOADING DATASET: xsp ************************"
    echo "!!WARNING!! please check prepare_xsp.py for more details."
else
    echo "xsp is already downloaded."
fi

# Spider-DK
if ! (test -d Spider-DK)
then
    echo "************************ DOWNLOADING DATASET: Spider-DK ************************"
    git clone https://github.com/ygan/Spider-DK.git
else
    echo "Spider-DK is already downloaded."
fi

# Spider-Syn
if ! (test -d Spider-Syn)
then
    echo "************************ DOWNLOADING DATASET: Spider-Syn ************************"
    git clone https://github.com/ygan/Spider-Syn.git
else
    echo "Spider-Syn is already downloaded."
fi

# squall
if ! (test -d squall)
then
    echo "************************ DOWNLOADING DATASET: squall ************************"
    git clone https://github.com/tzshi/squall.git
else
    echo "squall is already downloaded."
fi
