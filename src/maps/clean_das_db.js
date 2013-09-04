// clean-up parser DB
parser = db.getSisterDB("parser");
parser.db.drop();
// clean-up DAS DB
das = db.getSisterDB("das");
das.dropDatabase();
// clean-up mapping DB
mapping = db.getSisterDB("mapping");
mapping.dropDatabase();
