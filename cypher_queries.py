'''
Script for writing the cypher queries to the "queries.cyp" file
'''

#todo Dateien durchforsten, fuer jede Datei CREATEs machen und dann Daten mit LOAD CSV (key) importieren?

f'''
USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:/home/nina/PycharmProjects/chembl29/output_files/edge_tables/met_id-X-drug_record_id.tsv' AS line
CREATE (:Artist {name: line.{property_x}, year: toInteger(line.{property_y})})



#todo Herausfinden, ob es ueber MySQL oder Pandas eine Funktion gibt, um herauszufinden, wohin die Foreign Keys verbinden