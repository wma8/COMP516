import os
import Tables
import psycopg2
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from Tables import Authorship, Article, Inproceedings

class PostgreSQL:
    def __init__(self,
                 host: str = "localhost",
                 db_name: str = "dblp",
                 db_user: str = "dblpuser",
                 db_pass: str = "1234"
                 ):
        self.host = host
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass
        self.engine = sqlalchemy.create_engine('postgresql://'+self.db_user+':'+self.db_pass+'@'+self.host+'/'+self.db_name)
        self.con = self.engine.connect()
        self.Session = sessionmaker(bind=self.engine)
        self.session = self.Session()

    def close(self):
        self.con.close()

    def create_tables(self):
        Tables.create_tables(self.engine)

    def add_article(self, key, ti, jour, ye):
        art = self.session.query(Article).filter_by(pubkey=key).first()
        if not art:
            new_article = Article(pubkey=key, title=ti, journal=jour, year=ye)
            self.session.add(new_article)

    def add_proceed(self, key, ti, bti, ye):
        pro = self.session.query(Inproceedings).filter_by(pubkey=key).first()
        if not pro:
            new_proceed = Inproceedings(pubkey=key, title=ti, booktitle=bti, year=ye)
            self.session.add(new_proceed)

    def add_author(self, key, auth):
        aut = self.session.query(Authorship).filter_by(pubkey=key, author=auth).first()
        if not aut:
            new_author = Authorship(pubkey=key, author=auth)
            self.session.add(new_author)

    def commit(self):
        self.session.commit()