//! Predicate DSL and scope-predicate templates.
//!
//! [`ScopeExpr`] mirrors [`crate::ast::BoolExpr`] but its value positions are
//! [`Operand`]s — either a literal or a named parameter filled at bind time.
//! The free functions ([`col`], [`rel`], [`and`], …) build templates far terser
//! than hand-written `BoolExpr`. A [`crate::policy::ScopePolicy`] holds these
//! templates and resolves them against a [`Principal`] once per request.
//!
//! ```
//! use vision_graphql::predicate::{col, rel, principal};
//! // orders owned by the caller, samples reachable via their order:
//! let _orders = col("user_id").eq(principal());
//! let _samples = rel("order", col("user_id").eq(principal()));
//! ```

use std::collections::HashMap;

use serde_json::Value;

use crate::ast::{BoolExpr, CmpOp};
use crate::error::{Error, Result};

/// A value position in a scope template: a literal, or a named parameter
/// substituted at bind time. `principal` is the conventional default name.
#[derive(Debug, Clone)]
pub enum Operand {
    Lit(Value),
    Param(String),
}

impl From<Value> for Operand {
    fn from(v: Value) -> Self {
        Operand::Lit(v)
    }
}
impl From<&str> for Operand {
    fn from(v: &str) -> Self {
        Operand::Lit(Value::from(v))
    }
}
impl From<String> for Operand {
    fn from(v: String) -> Self {
        Operand::Lit(Value::from(v))
    }
}
impl From<i64> for Operand {
    fn from(v: i64) -> Self {
        Operand::Lit(Value::from(v))
    }
}
impl From<i32> for Operand {
    fn from(v: i32) -> Self {
        Operand::Lit(Value::from(v))
    }
}
impl From<bool> for Operand {
    fn from(v: bool) -> Self {
        Operand::Lit(Value::from(v))
    }
}
impl From<f64> for Operand {
    fn from(v: f64) -> Self {
        Operand::Lit(Value::from(v))
    }
}

/// A named parameter reference, e.g. `param("tenant_id")`.
pub fn param(name: impl Into<String>) -> Operand {
    Operand::Param(name.into())
}

/// The default-named parameter (`principal`). Sugar for `param("principal")`.
pub fn principal() -> Operand {
    Operand::Param("principal".into())
}

/// A scope predicate template — the shape of [`crate::ast::BoolExpr`] with
/// [`Operand`] value leaves. Build via the DSL ([`col`], [`rel`], …); resolve
/// against a [`Principal`] to get a concrete `BoolExpr`.
#[derive(Debug, Clone)]
pub enum ScopeExpr {
    And(Vec<ScopeExpr>),
    Or(Vec<ScopeExpr>),
    Not(Box<ScopeExpr>),
    Relation {
        name: String,
        inner: Box<ScopeExpr>,
    },
    Compare {
        column: String,
        op: CmpOp,
        value: Operand,
    },
    IsNull {
        column: String,
        negated: bool,
    },
    InList {
        column: String,
        values: Vec<Operand>,
        negated: bool,
    },
}

/// Start a column predicate: `col("user_id").eq(principal())`.
pub fn col(name: impl Into<String>) -> Col {
    Col(name.into())
}

/// Builder returned by [`col`]; finish with a comparison method.
pub struct Col(String);

impl Col {
    fn cmp(self, op: CmpOp, v: impl Into<Operand>) -> ScopeExpr {
        ScopeExpr::Compare {
            column: self.0,
            op,
            value: v.into(),
        }
    }
    pub fn eq(self, v: impl Into<Operand>) -> ScopeExpr {
        self.cmp(CmpOp::Eq, v)
    }
    pub fn neq(self, v: impl Into<Operand>) -> ScopeExpr {
        self.cmp(CmpOp::Neq, v)
    }
    pub fn gt(self, v: impl Into<Operand>) -> ScopeExpr {
        self.cmp(CmpOp::Gt, v)
    }
    pub fn gte(self, v: impl Into<Operand>) -> ScopeExpr {
        self.cmp(CmpOp::Gte, v)
    }
    pub fn lt(self, v: impl Into<Operand>) -> ScopeExpr {
        self.cmp(CmpOp::Lt, v)
    }
    pub fn lte(self, v: impl Into<Operand>) -> ScopeExpr {
        self.cmp(CmpOp::Lte, v)
    }
    pub fn like(self, v: impl Into<Operand>) -> ScopeExpr {
        self.cmp(CmpOp::Like, v)
    }
    pub fn ilike(self, v: impl Into<Operand>) -> ScopeExpr {
        self.cmp(CmpOp::ILike, v)
    }
    pub fn nlike(self, v: impl Into<Operand>) -> ScopeExpr {
        self.cmp(CmpOp::NLike, v)
    }
    pub fn nilike(self, v: impl Into<Operand>) -> ScopeExpr {
        self.cmp(CmpOp::NILike, v)
    }
    pub fn is_null(self) -> ScopeExpr {
        ScopeExpr::IsNull {
            column: self.0,
            negated: false,
        }
    }
    pub fn is_not_null(self) -> ScopeExpr {
        ScopeExpr::IsNull {
            column: self.0,
            negated: true,
        }
    }
    pub fn in_<I, T>(self, vs: I) -> ScopeExpr
    where
        I: IntoIterator<Item = T>,
        T: Into<Operand>,
    {
        ScopeExpr::InList {
            column: self.0,
            values: vs.into_iter().map(Into::into).collect(),
            negated: false,
        }
    }
    pub fn nin<I, T>(self, vs: I) -> ScopeExpr
    where
        I: IntoIterator<Item = T>,
        T: Into<Operand>,
    {
        ScopeExpr::InList {
            column: self.0,
            values: vs.into_iter().map(Into::into).collect(),
            negated: true,
        }
    }
}

/// An `EXISTS` relation predicate: `rel("order", col("user_id").eq(principal()))`.
pub fn rel(name: impl Into<String>, inner: ScopeExpr) -> ScopeExpr {
    ScopeExpr::Relation {
        name: name.into(),
        inner: Box::new(inner),
    }
}

/// Conjunction of sub-predicates.
pub fn and<I: IntoIterator<Item = ScopeExpr>>(parts: I) -> ScopeExpr {
    ScopeExpr::And(parts.into_iter().collect())
}

/// Disjunction of sub-predicates.
pub fn or<I: IntoIterator<Item = ScopeExpr>>(parts: I) -> ScopeExpr {
    ScopeExpr::Or(parts.into_iter().collect())
}

/// Negation.
pub fn not(e: ScopeExpr) -> ScopeExpr {
    ScopeExpr::Not(Box::new(e))
}

/// Per-request parameter bag. `principal` is the conventional default name; set
/// extra named params for multi-key scopes (e.g. `tenant_id`).
#[derive(Debug, Clone, Default)]
pub struct Principal {
    params: HashMap<String, Value>,
}

impl Principal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind `name` to `value`. Chainable.
    pub fn set(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.params.insert(name.into(), value.into());
        self
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.params.get(name)
    }
}

impl Operand {
    fn resolve(&self, p: &Principal) -> Result<Value> {
        match self {
            Operand::Lit(v) => Ok(v.clone()),
            Operand::Param(name) => p.get(name).cloned().ok_or_else(|| Error::Validate {
                path: format!("principal.{name}"),
                message: format!("scope parameter '{name}' not supplied"),
            }),
        }
    }
}

impl ScopeExpr {
    /// Resolve this template against `p`, producing a concrete `BoolExpr`.
    /// Errors only when a referenced parameter is missing from `p`.
    pub fn resolve(&self, p: &Principal) -> Result<BoolExpr> {
        Ok(match self {
            ScopeExpr::And(parts) => {
                BoolExpr::And(parts.iter().map(|e| e.resolve(p)).collect::<Result<_>>()?)
            }
            ScopeExpr::Or(parts) => {
                BoolExpr::Or(parts.iter().map(|e| e.resolve(p)).collect::<Result<_>>()?)
            }
            ScopeExpr::Not(inner) => BoolExpr::Not(Box::new(inner.resolve(p)?)),
            ScopeExpr::Relation { name, inner } => BoolExpr::Relation {
                name: name.clone(),
                inner: Box::new(inner.resolve(p)?),
            },
            ScopeExpr::Compare { column, op, value } => BoolExpr::Compare {
                column: column.clone(),
                op: *op,
                value: value.resolve(p)?,
            },
            ScopeExpr::IsNull { column, negated } => BoolExpr::IsNull {
                column: column.clone(),
                negated: *negated,
            },
            ScopeExpr::InList {
                column,
                values,
                negated,
            } => BoolExpr::InList {
                column: column.clone(),
                values: values.iter().map(|v| v.resolve(p)).collect::<Result<_>>()?,
                negated: *negated,
            },
        })
    }

    /// Resolve a placeholder-free template. Errors if any parameter remains —
    /// useful when feeding the DSL straight into a raw [`crate::ScopeSet`].
    pub fn literal(&self) -> Result<BoolExpr> {
        self.resolve(&Principal::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn literal_resolves_without_principal() {
        let expr = col("user_id").eq(7);
        let BoolExpr::Compare { column, value, .. } = expr.literal().unwrap() else {
            panic!("expected compare");
        };
        assert_eq!(column, "user_id");
        assert_eq!(value, json!(7));
    }

    #[test]
    fn param_resolves_from_principal() {
        let expr = rel("order", col("user_id").eq(principal()));
        let p = Principal::new().set("principal", 42);
        let BoolExpr::Relation { inner, .. } = expr.resolve(&p).unwrap() else {
            panic!("expected relation");
        };
        let BoolExpr::Compare { value, .. } = *inner else {
            panic!("expected compare");
        };
        assert_eq!(value, json!(42));
    }

    #[test]
    fn missing_param_errors() {
        let expr = col("tenant_id").eq(param("tenant_id"));
        let err = expr.resolve(&Principal::new()).unwrap_err();
        assert!(matches!(err, Error::Validate { .. }));
    }

    #[test]
    fn named_params_resolve_independently() {
        let expr = and([
            col("tenant_id").eq(param("tenant_id")),
            col("user_id").eq(param("user_id")),
        ]);
        let p = Principal::new().set("tenant_id", 1).set("user_id", 2);
        let BoolExpr::And(parts) = expr.resolve(&p).unwrap() else {
            panic!("expected and");
        };
        let vals: Vec<_> = parts
            .iter()
            .map(|e| match e {
                BoolExpr::Compare { value, .. } => value.clone(),
                _ => panic!("expected compare"),
            })
            .collect();
        assert_eq!(vals, vec![json!(1), json!(2)]);
    }
}
