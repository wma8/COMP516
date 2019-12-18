#!/usr/bin/python
import xml.sax
from Connector import PostgreSQL

class ArticleHandler( xml.sax.ContentHandler ):
    def __init__(self):
        self.CurrentData = ""
        self.pubkey = ""
        self.author = ""
        self.title = ""
        self.year = ""
        # Alternative 
        self.journal = ""
        self.booktitle = ""
        # postgreSQL
        self.connector = PostgreSQL()
        self.counter = 0
   # Call when an element starts
    def startElement(self, tag, attributes):
        self.CurrentData = tag
        if tag == "article":
            self.pubkey = attributes["key"]
        elif tag == "inproceedings":
            self.pubkey = attributes["key"]

   # Call when an elements ends
    def endElement(self, tag):
        if tag == "article":
            self.connector.add_article(self.pubkey, self.title, self.journal, self.year)
            self.counter = self.counter + 1
        elif tag == "inproceedings":
            # put the 4 assigned data into the DB
            self.connector.add_proceed(self.pubkey, self.title, self.booktitle, self.year)
            self.counter = self.counter + 1
        if self.counter >= 10000:
            self.connector.commit()
            self.counter = 0
            print(self.counter)
        self.CurrentData = ""

   # Call when a character is read
    def characters(self, content):
        if self.CurrentData == "author":
            self.author = content
        elif self.CurrentData == "title":
            self.title = content
        elif self.CurrentData == "journal":
            self.journal = content
        elif self.CurrentData == "booktitle":
            self.booktitle = content
        elif self.CurrentData == "year":
            self.year = int(content)

# if __name__ == "__main__":
    
#     # create an XMLReader
#     parser = xml.sax.make_parser()
#     # turn off namepsaces
#     parser.setFeature(xml.sax.handler.feature_namespaces, 0)

#     Handler = ArticleHandler()
#     parser.setContentHandler( Handler )
    
#     parser.parse("dblp.xml")
#     Handler.connector.commit()
#     Handler.connector.close()
