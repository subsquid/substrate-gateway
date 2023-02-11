use sqlx::postgres::{PgArguments, Postgres};
use sqlx::{Arguments as ArgumentsTrait, Encode, Type};
use std::fmt;

pub fn select<T>(columns: T) -> Select
where
    T: IntoIterator,
    T::Item: Into<String>,
{
    Select {
        columns: columns.into_iter().map(|c| c.into()).collect(),
        from: String::new(),
        conditions: vec![],
        order_by: None,
    }
}

#[derive(Default)]
pub struct Parameters {
    inner: PgArguments,
    count: usize,
}

impl Parameters {
    pub fn add<'a, T>(&mut self, value: T) -> String
    where
        T: Encode<'a, Postgres> + Type<Postgres> + Send + 'a,
    {
        self.count += 1;
        self.inner.add(value);
        format!("${}", self.count)
    }

    pub fn get(self) -> PgArguments {
        self.inner
    }
}

pub struct Select {
    columns: Vec<String>,
    from: String,
    conditions: Vec<String>,
    order_by: Option<String>,
}

impl Select {
    pub fn from(mut self, table: impl Into<String>) -> Select {
        self.from = table.into();
        self
    }

    pub fn where_(mut self, condition: impl Into<String>) -> Select {
        self.conditions.push(condition.into());
        self
    }

    pub fn order_by(mut self, expr: impl Into<String>) -> Select {
        self.order_by = Some(expr.into());
        self
    }
}

impl fmt::Display for Select {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT")?;
        write!(f, " {}", self.columns.join(", "))?;
        write!(f, " FROM {}", &self.from)?;
        if !self.conditions.is_empty() {
            write!(f, " WHERE {}", self.conditions.join(" AND "))?;
        }
        if let Some(order_by) = &self.order_by {
            write!(f, " ORDER BY {}", order_by)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::{select, Parameters};

    #[test]
    fn display_base_query() {
        let mut params = Parameters::default();
        let query = select(["col1", "col2"])
            .from("table")
            .where_(format!("col1 = {}", params.add(1)))
            .order_by("col1")
            .to_string();
        assert_eq!(
            query,
            "SELECT col1, col2 FROM table WHERE col1 = $1 ORDER BY col1"
        );
    }
}
