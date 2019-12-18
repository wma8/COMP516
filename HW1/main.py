from XMLhandler import ArticleHandler
from authorparser import AuthorHandler
import xml.sax

if __name__ == '__main__':
    # create an XMLReader
    parser = xml.sax.make_parser()
    # turn off namepsaces
    parser.setFeature(xml.sax.handler.feature_namespaces, 0)

    # override the default ContextHandler
    Handler = ArticleHandler()
    parser.setContentHandler( Handler )
    
    parser.parse("dblp.xml")
    Handler.connector.commit()
    Handler.connector.close()


    Handler = AuthorHandler()
    parser.setContentHandler( Handler )

    parser.parse("dblp.xml")
    Handler.connector.commit()
    Handler.connector.close()

    