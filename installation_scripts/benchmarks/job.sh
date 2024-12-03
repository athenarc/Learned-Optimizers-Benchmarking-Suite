# Prerequisite: imdbload database

sudo apt install python3 python3-pip python3-venv wget python3-dev libpq-dev git
wget ftp://ftp.fu-berlin.de/misc/movies/database/frozendata/*gz
mkdir job-zips
mv *.gz job-zips/

git clone https://github.com/cinemagoer/cinemagoer.git
cd cinemagoer/
python3 -m venv myenv
source myenv/bin/activate

pip install git+https://github.com/cinemagoer/cinemagoer
pip install psycopg2
pip install -r requirements.txt
pip install --upgrade sqlalchemy==1.4
python3 bin/imdbpy2sql.py -d ../job-zips/ -u "postgresql+psycopg2://suite_user:71Vgfi4mUNPm@localhost/imdbload?client_encoding=UTF8"