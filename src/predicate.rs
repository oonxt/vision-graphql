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

use crate::ast::{BoolExpr, CmpOp, Val};
use crate::error::{Error, Result};
use crate::schema::PgType;

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

/// A named parameter reference, e.g. `param("tenant_id")`. A dotted name —
/// `param("claim.school_id")` — reads that field of an object-valued parameter;
/// see [`Principal::get`] for the lookup rule. Every segment must be an
/// identifier (`[A-Za-z_][A-Za-z0-9_]*`), which
/// [`validate`](crate::policy::ScopePolicyBuilder::validate) checks — the same
/// grammar the TOML loader applies to `"$name"`.
pub fn param(name: impl Into<String>) -> Operand {
    Operand::Param(name.into())
}

/// Whether `s` is a well-formed parameter reference: `ident(.ident)*`, each
/// `ident` matching `[A-Za-z_][A-Za-z0-9_]*`.
///
/// One grammar for both entry points. The TOML loader applies it to decide what
/// a `"$…"` string is; `validate` applies it to every `Operand::Param` the DSL
/// built, so a name that can never resolve is refused when the policy is built
/// rather than on every request, blamed on the caller's principal.
pub(crate) fn is_param_ref(s: &str) -> bool {
    s.split('.').all(is_ident)
}

fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
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
    /// `column` in a list that is one operand: a parameter bound to a JSON
    /// array (`col("status").in_set(param("states"))`) or a literal array.
    /// [`InList`](ScopeExpr::InList) lists its members one operand each;
    /// this is for a list whose members only the principal knows.
    InSet {
        column: String,
        set: Operand,
        negated: bool,
    },
    /// `TRUE` / `FALSE`; see [`constant`].
    Const(bool),
    /// A comparison with no column on either side; see [`typed`].
    ValueCompare {
        left: Operand,
        op: CmpOp,
        right: Operand,
        ty: PgType,
    },
    /// A membership test with no column; see [`Typed::in_set`].
    ValueInSet {
        value: Operand,
        set: Operand,
        ty: PgType,
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
    /// `column` is a member of `set`, one operand resolving to a list:
    /// `col("status").in_set(param("states"))` for a principal that carries
    /// the list. See [`ScopeExpr::InSet`].
    pub fn in_set(self, set: impl Into<Operand>) -> ScopeExpr {
        ScopeExpr::InSet {
            column: self.0,
            set: set.into(),
            negated: false,
        }
    }
    /// `column` is not a member of `set`; see [`Col::in_set`].
    pub fn nin_set(self, set: impl Into<Operand>) -> ScopeExpr {
        ScopeExpr::InSet {
            column: self.0,
            set: set.into(),
            negated: true,
        }
    }
}

/// `TRUE` (`constant(true)`) or `FALSE`. The leaf a policy lowers "sees
/// everything" or "sees nothing" to — the same SQL as `and([])` / `or([])`,
/// but said rather than implied by an empty list.
pub fn constant(value: bool) -> ScopeExpr {
    ScopeExpr::Const(value)
}

/// Start a predicate over a value that is not a column:
/// `typed(param("role"), PgType::Text).eq("admin")`.
///
/// A column comparison takes its bind type from the column. Here there is no
/// column — the whole point is a condition on the principal alone, "an admin
/// sees every row" — so `ty` says what the operands bind as. It is kept in the
/// SQL as `$n::ty = $m::ty` rather than folded by the host so that one
/// statement compiled against the policy serves every principal.
pub fn typed(value: impl Into<Operand>, ty: PgType) -> Typed {
    Typed(value.into(), ty)
}

/// Builder returned by [`typed`]; finish with a comparison method.
pub struct Typed(Operand, PgType);

impl Typed {
    fn cmp(self, op: CmpOp, v: impl Into<Operand>) -> ScopeExpr {
        ScopeExpr::ValueCompare {
            left: self.0,
            op,
            right: v.into(),
            ty: self.1,
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
    /// The value is a member of `set`, one operand resolving to a list:
    /// `typed("vip", PgType::Text).in_set(param("tiers"))`.
    pub fn in_set(self, set: impl Into<Operand>) -> ScopeExpr {
        ScopeExpr::ValueInSet {
            value: self.0,
            set: set.into(),
            ty: self.1,
            negated: false,
        }
    }
    /// The value is not a member of `set`; see [`Typed::in_set`].
    pub fn nin_set(self, set: impl Into<Operand>) -> ScopeExpr {
        ScopeExpr::ValueInSet {
            value: self.0,
            set: set.into(),
            ty: self.1,
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

    /// Bind `name` to `value`. Chainable. An object value can be read field by
    /// field from a policy (`param("claim.school_id")`, TOML
    /// `"$claim.school_id"`); see [`Principal::get`].
    pub fn set(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.params.insert(name.into(), value.into());
        self
    }

    /// Look up a parameter reference: a bound name, or a dotted path into a
    /// bound object (`claim.school_id` reads field `school_id` of parameter
    /// `claim`).
    ///
    /// The whole reference is tried as a bound name first, so a host that set
    /// the key `"claim.school_id"` itself gets exactly what it set; otherwise
    /// the text before the first dot names the parameter and the rest is a
    /// path of object keys. Only that one split is tried: a key that itself
    /// contains a dot cannot be addressed by a path, and a verbatim-bound
    /// `"claim.org"` is not consulted for `claim.org.id`. Walking stops at
    /// anything that is not an object — `None`, not `Null`, because a field
    /// that is not there and a field that is null are different answers. A
    /// null that is there is handed on, and the predicate then treats it as it
    /// treats any null value (a comparison refuses it).
    pub fn get(&self, name: &str) -> Option<&Value> {
        if let Some(v) = self.params.get(name) {
            return Some(v);
        }
        let (head, rest) = name.split_once('.')?;
        rest.split('.')
            .try_fold(self.params.get(head)?, |v, key| v.as_object()?.get(key))
    }
}

impl Operand {
    fn collect_param(&self, out: &mut std::collections::BTreeSet<String>) {
        if let Operand::Param(name) = self {
            out.insert(name.clone());
        }
    }

    /// This operand as a [`Val`], keeping parameters unresolved.
    pub(crate) fn symbolic(&self) -> Val {
        match self {
            Operand::Lit(v) => Val::Lit(v.clone()),
            Operand::Param(name) => Val::ScopeParam(name.clone()),
        }
    }

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
    /// Append every parameter name this template references to `out`.
    pub(crate) fn collect_params(&self, out: &mut std::collections::BTreeSet<String>) {
        match self {
            ScopeExpr::And(parts) | ScopeExpr::Or(parts) => {
                for p in parts {
                    p.collect_params(out);
                }
            }
            ScopeExpr::Not(inner) | ScopeExpr::Relation { inner, .. } => inner.collect_params(out),
            ScopeExpr::Compare { value, .. } | ScopeExpr::InSet { set: value, .. } => {
                value.collect_param(out);
            }
            ScopeExpr::IsNull { .. } | ScopeExpr::Const(_) => {}
            ScopeExpr::InList { values, .. } => {
                for v in values {
                    v.collect_param(out);
                }
            }
            ScopeExpr::ValueCompare { left, right, .. } => {
                left.collect_param(out);
                right.collect_param(out);
            }
            ScopeExpr::ValueInSet { value, set, .. } => {
                value.collect_param(out);
                set.collect_param(out);
            }
        }
    }

    /// Resolve this template against `p`, producing a concrete `BoolExpr`.
    /// Errors only when a referenced parameter is missing from `p`.
    pub fn resolve(&self, p: &Principal) -> Result<BoolExpr> {
        self.lower(&|o| o.resolve(p).map(Val::Lit))
    }

    /// Lower this template *without* a principal, leaving each parameter as a
    /// [`Val::ScopeParam`] for the request to supply.
    ///
    /// This is what lets a query be compiled against a policy rather than
    /// against one caller: the predicate is baked into the SQL, but whose rows
    /// it selects is still decided per request. Compiling per principal would
    /// give every tenant its own copy of the same statement.
    pub fn symbolic(&self) -> BoolExpr {
        self.lower(&|o| Ok(o.symbolic()))
            .expect("symbolic lowering has no parameter to miss")
    }

    /// The one walk both lowerings are: the shape is copied, and every
    /// operand goes through `leaf`.
    fn lower(&self, leaf: &dyn Fn(&Operand) -> Result<Val>) -> Result<BoolExpr> {
        Ok(match self {
            ScopeExpr::And(parts) => {
                BoolExpr::And(parts.iter().map(|e| e.lower(leaf)).collect::<Result<_>>()?)
            }
            ScopeExpr::Or(parts) => {
                BoolExpr::Or(parts.iter().map(|e| e.lower(leaf)).collect::<Result<_>>()?)
            }
            ScopeExpr::Not(inner) => BoolExpr::Not(Box::new(inner.lower(leaf)?)),
            ScopeExpr::Relation { name, inner } => BoolExpr::Relation {
                name: name.clone(),
                inner: Box::new(inner.lower(leaf)?),
            },
            ScopeExpr::Compare { column, op, value } => BoolExpr::Compare {
                column: column.clone(),
                op: *op,
                value: leaf(value)?,
            },
            ScopeExpr::IsNull { column, negated } => BoolExpr::is_null(column.clone(), !*negated),
            ScopeExpr::InList {
                column,
                values,
                negated,
            } => BoolExpr::InList {
                column: column.clone(),
                values: Val::Array(values.iter().map(leaf).collect::<Result<_>>()?).collapse(),
                negated: *negated,
            },
            ScopeExpr::InSet {
                column,
                set,
                negated,
            } => BoolExpr::InList {
                column: column.clone(),
                values: leaf(set)?,
                negated: *negated,
            },
            ScopeExpr::Const(b) => BoolExpr::Const(*b),
            ScopeExpr::ValueCompare {
                left,
                op,
                right,
                ty,
            } => BoolExpr::ValueCompare {
                left: leaf(left)?,
                op: *op,
                right: leaf(right)?,
                pg: ty.clone(),
            },
            ScopeExpr::ValueInSet {
                value,
                set,
                ty,
                negated,
            } => BoolExpr::ValueInList {
                value: leaf(value)?,
                values: leaf(set)?,
                pg: ty.clone(),
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
    fn dotted_param_reads_into_a_bound_object() {
        let expr = col("school_id").eq(param("claim.school_id"));
        let p = Principal::new().set("claim", json!({"school_id": 12, "role": "staff"}));
        let BoolExpr::Compare { value, .. } = expr.resolve(&p).unwrap() else {
            panic!("expected compare");
        };
        assert_eq!(value, json!(12));

        // A path can go more than one level down.
        let p = Principal::new().set("claim", json!({"org": {"id": 3}}));
        let BoolExpr::Compare { value, .. } =
            col("org_id").eq(param("claim.org.id")).resolve(&p).unwrap()
        else {
            panic!("expected compare");
        };
        assert_eq!(value, json!(3));
    }

    #[test]
    fn dotted_param_missing_field_is_an_error_not_null() {
        // The object is bound but lacks the field: fail closed, exactly like an
        // unbound flat parameter, rather than comparing against null.
        let expr = col("school_id").eq(param("claim.school_id"));
        let p = Principal::new().set("claim", json!({"role": "staff"}));
        let err = expr.resolve(&p).unwrap_err();
        assert!(
            matches!(err, Error::Validate { ref path, .. } if path == "principal.claim.school_id"),
            "{err:?}"
        );

        // Walking into a non-object is the same: nothing to read.
        let p = Principal::new().set("claim", json!("opaque"));
        assert!(expr.resolve(&p).is_err());

        // An explicit null field is a value, and is handed on as one.
        let p = Principal::new().set("claim", json!({"school_id": null}));
        let BoolExpr::Compare { value, .. } = expr.resolve(&p).unwrap() else {
            panic!("expected compare");
        };
        assert_eq!(value, Value::Null);
    }

    #[test]
    fn a_dotted_key_bound_verbatim_wins_over_the_walk() {
        let p = Principal::new()
            .set("claim.school_id", 1)
            .set("claim", json!({"school_id": 2}));
        assert_eq!(p.get("claim.school_id"), Some(&json!(1)));
        // Only the whole reference is tried verbatim: a bound dotted prefix is
        // not a parameter the path can start from.
        let p = Principal::new().set("claim.org", json!({"id": 3}));
        assert_eq!(p.get("claim.org.id"), None);
    }

    #[test]
    fn param_ref_grammar() {
        for ok in ["principal", "_t1", "claim.school_id", "a.b.c"] {
            assert!(is_param_ref(ok), "{ok}");
        }
        for bad in [
            "", "1st", "claim.", ".x", "a..b", "a b", "a-b", "{a}", "a:b",
        ] {
            assert!(!is_param_ref(bad), "{bad}");
        }
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

    #[test]
    fn typed_leaf_keeps_both_operands_and_its_type() {
        let expr = typed(param("role"), PgType::Text).eq("admin");
        // Symbolic: the parameter survives to the request.
        let BoolExpr::ValueCompare {
            left, right, pg, ..
        } = expr.symbolic()
        else {
            panic!("expected value compare");
        };
        assert!(matches!(left, Val::ScopeParam(ref n) if n == "role"));
        assert_eq!(right, json!("admin"));
        assert_eq!(pg, PgType::Text);
        // Resolved: both sides literal.
        let p = Principal::new().set("role", "admin");
        let BoolExpr::ValueCompare { left, .. } = expr.resolve(&p).unwrap() else {
            panic!("expected value compare");
        };
        assert_eq!(left, json!("admin"));
        assert!(expr.resolve(&Principal::new()).is_err());
    }

    #[test]
    fn set_leaves_take_the_whole_list_from_one_operand() {
        let p = Principal::new().set("tiers", json!(["vip", "gold"]));
        let BoolExpr::ValueInList { value, values, .. } = typed("vip", PgType::Text)
            .in_set(param("tiers"))
            .resolve(&p)
            .unwrap()
        else {
            panic!("expected value in list");
        };
        assert_eq!(value, json!("vip"));
        assert_eq!(values, json!(["vip", "gold"]));

        let BoolExpr::InList { column, values, .. } = col("tier").in_set(param("tiers")).symbolic()
        else {
            panic!("expected in list");
        };
        assert_eq!(column, "tier");
        assert!(matches!(values, Val::ScopeParam(ref n) if n == "tiers"));
    }

    #[test]
    fn constant_and_typed_leaves_report_their_params() {
        let expr = and([
            constant(true),
            typed(param("role"), PgType::Text).eq(param("wanted")),
            typed("vip", PgType::Text).in_set(param("tiers")),
            col("status").in_set(param("states")),
        ]);
        let mut out = std::collections::BTreeSet::new();
        expr.collect_params(&mut out);
        assert_eq!(
            out.into_iter().collect::<Vec<_>>(),
            ["role", "states", "tiers", "wanted"]
        );
        assert!(matches!(
            constant(false).literal().unwrap(),
            BoolExpr::Const(false)
        ));
    }
}
