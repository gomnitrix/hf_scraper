// Create database and collections
db = db.getSiblingDB('huggingface_scraper');

// Create collections
db.createCollection('models');
db.createCollection('datasets');

// Create indexes
db.models.createIndex({ "_id": 1 }, { unique: true });
db.models.createIndex({ "basic_metadata.tags": 1 });
db.models.createIndex({ "status.phase": 1 });

db.datasets.createIndex({ "_id": 1 }, { unique: true });
db.datasets.createIndex({ "basic_metadata.tags": 1 });
db.datasets.createIndex({ "status.phase": 1 });

// Create user for application
db.createUser({
  user: 'scraper',
  pwd: 'scraper_password',
  roles: [
    {
      role: 'readWrite',
      db: 'huggingface_scraper'
    }
  ]
}); 