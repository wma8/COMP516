# !/usr/bin/python
import xml.sax
from Connector import PostgreSQL
# article|inproceedings|proceedings|book|incollection|
                # phdthesis|mastersthesis|www|person|data
class AuthorHandler( xml.sax.ContentHandler ):
    def __init__(self):
        self.CurrentData = ""
        self.tags = ""
        self.pubkey = ""
        self.authors = []
        self.subauthor = []

        self.connector = PostgreSQL()
        self.counter = 0
   # Call when an element starts
    def startElement(self, tag, attributes):
        self.CurrentData = tag
        if self.CurrentData == "article" or self.CurrentData == "inproceedings":
            self.pubkey = attributes["key"]
        elif self.CurrentData == "proceedings" or self.CurrentData == "book" or self.CurrentData == "incollection" or self.CurrentData == "phdthesis" or self.CurrentData == "mastersthesis" or self.CurrentData == "www" or self.CurrentData == "person" or self.CurrentData == "data":
            self.pubkey = ""

   # Call when an elements ends
    def endElement(self, tag):
        if self.CurrentData == "author":
            temp = ""
            for sub in self.subauthor:
                temp = temp + sub
            self.authors.append(temp)
            self.subauthor = []
        if tag == "article" or tag == "inproceedings":
            for author in self.authors:
                self.connector.add_author(self.pubkey, author)
                self.counter = self.counter + 1
                # self.gate.write(self.pubkey + "/ " + author + "\n")
            self.authors = []
            print(self.counter)
        if self.counter >= 10000:
            self.connector.commit()
            self.counter = 0
            
        self.CurrentData = ""

   # Call when a character is read
    def characters(self, content):
        if self.CurrentData == "author" and self.pubkey != "":
            self.subauthor.append(content)
 

# if __name__ == "__main__":
    
#     # create an XMLReader
#     parser = xml.sax.make_parser()
#     # turn off namepsaces
#     parser.setFeature(xml.sax.handler.feature_namespaces, 0)

#     # override the default ContextHandler

#     Handler = AuthorHandler()
#     # Handler.connector.create_tables()
#     parser.setContentHandler( Handler )

#     parser.parse("dblp.xml")
#     Handler.connector.commit()
#     Handler.connector.close()

