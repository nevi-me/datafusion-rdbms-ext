use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use datafusion::{catalog, datasource::TableProvider, error::*};

pub struct DatabaseCatalog {
    database_name: String,
    schemas: RwLock<HashMap<String, Arc<dyn catalog::schema::SchemaProvider>>>,
}

impl DatabaseCatalog {
    pub fn new(database_name: &str) -> Self {
        Self {
            database_name: database_name.to_string(),
            schemas: RwLock::default(),
        }
    }
}

impl catalog::catalog::CatalogProvider for DatabaseCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read().unwrap();
        schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<std::sync::Arc<dyn catalog::schema::SchemaProvider>> {
        let schemas = self.schemas.read().unwrap();
        schemas.get(name).cloned()
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn catalog::schema::SchemaProvider>,
    ) -> Option<Arc<dyn catalog::schema::SchemaProvider>> {
        let mut schemas = self.schemas.write().unwrap();
        schemas.insert(name.into(), schema)
    }
}

pub struct SchemaCatalog {
    // TODO: this might not be needed, I left it here in case there's use for it
    schema_name: String,
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl SchemaCatalog {
    pub fn new(schema_name: &str) -> Self {
        Self {
            schema_name: schema_name.to_string(),
            tables: RwLock::default(),
        }
    }
}

impl catalog::schema::SchemaProvider for SchemaCatalog {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        let tables = self.tables.read().unwrap();
        tables.get(name).cloned()
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.read().unwrap();
        tables.contains_key(name)
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "The table {} already exists",
                name
            )));
        }
        let mut tables = self.tables.write().unwrap();
        Ok(tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write().unwrap();
        Ok(tables.remove(name))
    }
}
