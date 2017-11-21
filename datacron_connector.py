import pandas as pd
from pandas.io.json import json_normalize, read_json
from SPARQLWrapper import SPARQLWrapper, JSON, XML, RDF


class TripleStoreConnector():
    """
    Provides an SPARQL Wrapper interface to the datAcron TripleStore.
    """
    
    def __init__(self, sparql_selector=0):
        """
        @param sparql_selector: select the triple store to connect to. 0 for 107:8890, otherwise 109:3434.
        """
        if sparql_selector == 0:
            self.sparql_url = 'http://83.212.239.107:8890/sparql'
            self.sparql = SPARQLWrapper(self.sparql_url)
        else:
            self.sparql_url = 'http://83.212.239.109:3434/main.tdb/query'
            self.sparql = SPARQLWrapper(self.sparql_url)
            self.sparql.setCredentials('admin', 'pw')
            #self.sparql.setRequestMethod('postdirectly')
            #self.sparql.setMethod('POST')
            
        self.prefixes = ['http://www.datacron-project.eu/datAcron#']
        self.prefixes.append('http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#')
        self.prefixes.append('http://www.w3.org/2001/XMLSchema#')
        self.prefixes.append('http://www.w3.org/2000/01/rdf-schema#')
        self.prefixes.append('java:datAcronTester.unipi.gr.sparql_functions.')
        self.prefixes.append('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
        self.prefixes.append('http://www.openlinksw.com/schemas/virtrdf#')
        self.prefixes.append('http://www.w3.org/2002/07/owl#')
           
    
    def raw_query(self, query):
        """
        Query the selected endpoint with the given query string and return the results as a pandas Dataframe.
        @param query: the SPARQL query in a string format.
        """
        # create the connection to the endpoint; set return format; ask for result  
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        result = self.sparql.query().convert()
        return result
    
    
    def query(self, query):
        """
        Query the selected endpoint with the given query string and return the results as a pandas Dataframe.
        @param query: the SPARQL query in a string format.
        """
        # create the connection to the endpoint; set return format; ask for result  
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        
        result = self.sparql.query().convert()

        #clean up the column mess (thanks to David Knodt)
        for row in result['results']['bindings']:
            for key in row.keys():
                row[key] = row[key]['value']            
        if len(result["results"]["bindings"]):
            dftemp = pd.DataFrame(result["results"]["bindings"])   #Prob: Pandas DataFrame() method sorted cols alphabetically
            dftemp = dftemp[result['head']['vars']]                #Solution: this reorders colmns as returned from SPARQL
            return dftemp
        else:
            return pd.DataFrame(columns=(result['head']['vars']))
     
        
    def clean(self, df):
        """
        Drop URI prefixes from result values. 
        @param df: a Pandas DataFrame.
        """
        if type(df) != pd.core.frame.DataFrame:
            print('Query Cleaning nicht möglich - es wurde kein Pandas DataFrame übergeben.')
            print ('der Typ des Objekts ist ein {}'.format(type(df)) )
            return df
        
        for column in df:
            for prefix in self.prefixes:
                    df[column] = df[column].str.replace(prefix, '')
        return df
    
    
    
  
    def regexify(self, df, column):
        """
        Creates a REGEX expression which matches the values of a pd.DataFrame column OR-wise.
        E.g. the pattern matches if the search string contains one or more elements of the column.
        With this, I can put the results of a query as parameters in a new query.
        """
        regex = '('
        for i in range (len(df)):
            regex = regex + df.iloc[i][column] + '|'

        regex = regex[:-3] + ')'
        return regex 
    
    def qry_properties(self, itemname, with_values=False):
        """
        Executes a SPARQL query and returns all properties of the given object: itemname ?p ?o.
        @param itemname: a string which contains the name of the RDF Subject.
        returns: a Pandas DataFrame.
        """
        
        # select proper prefixes for the different triple stores
        if self.sparql_url == 'http://83.212.239.107:8890/sparql':
            qry = """
            PREFIX : <http://www.datacron-project.eu/datAcron#>
            PREFIX dul: <http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> """
        else:
            qry = """
            PREFIX : <http://www.datacron-project.eu/datAcron#>
            PREFIX dul: <http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            PREFIX bif: <java:datAcronTester.unipi.gr.sparql_functions.>   """
            
        #build query
        if with_values:
            qry = qry + """
            SELECT ?p ?o
            WHERE { """ + str(itemname) + """  ?p ?o. } 
            """
        else:
            qry = qry + """
            SELECT ?p
            WHERE { """ + str(itemname) + """  ?p ?o. } 
            """           
        
        return self.clean(self.query(qry))
    
    def wkt_to_geojson(self, dataframe, wkt_column_name, geojson_column_name):
        """
        Gets a pandas.DataFrame and converts a WKT column into a GeoJSON column.
        The GeoJSON column then contains a dict.

        Arguments:
        dataframe -- the pandas.DataFrame which should be altered
        wkt_column_name -- the name of the column containing the well-known-text
        geojson_column_name -- how the GeoJSON column shall be named

        """
        if type(dataframe) != pd.core.frame.DataFrame:
            raise TypeError('The parameter dataframe must be a Pandas DataFrame.')
        if not wkt_column_name in dataframe.columns:
            raise ValueError('No column with the name ' + wkt_column_name + ' found in the DataFrame')

        dataframe[geojson_column_name] = dataframe[wkt_column_name].apply(lambda x: wkt.loads(x))
        return dataframe

    def calculate_center(self, geoJson):
        """
        Takeos a geoJson and calculates the center and returns it as long/lat coordinate.
        """
        lonmin, lonmax, latmin, latmax = 99, 0, 99, 0
        for point in  geoJson['coordinates'][0]:
            lonmin = min(point[0], lonmin)
            latmin = min(point[1], latmin)
            lonmax = max(point[0], lonmax)
            latmax = max(point[1], latmax)

        center = [lonmin + (lonmax - lonmin)/2 , latmin + (latmax - latmin)/2]
        return center
        #df_open_airblocks.iloc[0]['simpleJSON']['coordinates'][0]:

    def center_coordinate(self, dataframe, geojson_column_name, center_column_name):
        """
        Gets a pandas.DataFrame and calculates the center of a geoJson which should be a square.
        """
        if type(dataframe) != pd.core.frame.DataFrame:
            raise TypeError('The parameter dataframe must be a Pandas DataFrame.')
        if not geojson_column_name in dataframe.columns:
            raise ValueError('No column with the specified name found in the DataFrame')

        dataframe[center_column_name] = dataframe[geojson_column_name].apply(lambda x: calculate_center(x))
        return dataframe



        
            
        
        
        
        
        
        
        