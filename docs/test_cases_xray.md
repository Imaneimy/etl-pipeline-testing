# Test Cases — ETL Pipeline Testing
> Format compatible XRAY (Jira Test Management)

---

## Epic : ETL-PIPELINE | Sprint : Sprint-01

---

### TC-ETL-001 | Normalize column names

| Champ         | Valeur |
|---------------|--------|
| **Type**      | Unit |
| **Priorité**  | High |
| **Composant** | Transform |
| **Statut**    | Ready |

**Préconditions :** DataFrame avec colonnes ayant des majuscules et espaces.

**Étapes :**
1. Créer un DataFrame avec la colonne `"Product ID"`
2. Appeler `normalize_column_names(df)`
3. Vérifier que la colonne se nomme `"product_id"`

**Résultat attendu :** Toutes les colonnes sont en minuscule, espaces remplacés par `_`.

---

### TC-ETL-002 | Cast types — conversion valide

| Champ         | Valeur |
|---------------|--------|
| **Type**      | Unit |
| **Priorité**  | High |
| **Composant** | Transform |

**Préconditions :** Colonne `amount` de type StringType.

**Étapes :**
1. Appeler `cast_types(df, {"amount": "double"})`
2. Vérifier que le type de la colonne est `DoubleType`
3. Vérifier qu'aucune valeur n'est perdue

**Résultat attendu :** Colonne `amount` castée correctement sans perte de données.

---

### TC-ETL-003 | Cast types — valeur invalide devient NULL

| Champ         | Valeur |
|---------------|--------|
| **Type**      | Unit |
| **Priorité**  | Medium |
| **Composant** | Transform |

**Préconditions :** Colonne `amount` contenant `"NOT_A_NUMBER"`.

**Étapes :**
1. Appeler `cast_types(df, {"amount": "double"})`
2. Compter les NULLs dans la colonne résultante

**Résultat attendu :** 1 valeur NULL (la valeur non-numérique).

---

### TC-ETL-004 | Déduplication — doublons exacts

| Champ         | Valeur |
|---------------|--------|
| **Type**      | Unit |
| **Priorité**  | High |
| **Composant** | Transform |

**Préconditions :** DataFrame avec 3 lignes dont 2 identiques.

**Étapes :**
1. Appeler `drop_duplicates(df)`
2. Vérifier le nombre de lignes résultantes

**Résultat attendu :** 2 lignes uniques retournées.

---

### TC-ETL-005 | Déduplication — sous-ensemble de clés

| Champ         | Valeur |
|---------------|--------|
| **Type**      | Unit |
| **Priorité**  | Medium |
| **Composant** | Transform |

**Préconditions :** DataFrame avec `product_id` dupliqué mais `amount` différent.

**Étapes :**
1. Appeler `drop_duplicates(df, subset=["product_id"])`
2. Vérifier que chaque `product_id` n'apparaît qu'une fois

**Résultat attendu :** Une seule occurrence par `product_id`.

---

### TC-ETL-006 | Gestion des NULLs — stratégie DROP

| Champ         | Valeur |
|---------------|--------|
| **Type**      | Unit |
| **Priorité**  | High |
| **Composant** | Transform |

**Résultat attendu :** Lignes avec NULL dans colonne `product_id` supprimées.

---

### TC-ETL-007 | Gestion des NULLs — remplacement par constante

| Champ         | Valeur |
|---------------|--------|
| **Type**      | Unit |
| **Priorité**  | Medium |
| **Composant** | Transform |

**Résultat attendu :** NULLs remplacés par la valeur `0.0`.

---

### TC-ETL-008 | Métadonnées d'ingestion ajoutées

| Champ         | Valeur |
|---------------|--------|
| **Type**      | Unit |
| **Priorité**  | Low |
| **Composant** | Transform |

**Résultat attendu :** Colonnes `_ingestion_ts` et `_source` présentes dans le DataFrame.

---

### TC-ETL-009 | Agrégation des ventes — regroupement correct

| Champ         | Valeur |
|---------------|--------|
| **Type**      | Unit |
| **Priorité**  | High |
| **Composant** | Transform / Business Rules |

**Résultat attendu :** `total_amount = 300.0` pour P1/NORTH, `transaction_count = 2`.

---

### TC-ETL-010 | Agrégation sur DataFrame vide

| Champ         | Valeur |
|---------------|--------|
| **Type**      | Unit |
| **Priorité**  | Medium |
| **Composant** | Transform |

**Résultat attendu :** Retourne un DataFrame vide sans erreur.

---

### TC-VAL-001 → TC-VAL-010 | Validators (Schema + Business Rules)

Voir `tests/unit/test_validators.py` — mêmes conventions XRAY.

---

### TC-INT-001 | Pipeline complet sans erreur

| Champ         | Valeur |
|---------------|--------|
| **Type**      | Integration |
| **Priorité**  | Critical |
| **Composant** | Pipeline End-to-End |

**Préconditions :** Fichier `data/raw/sales.csv` présent.

**Résultat attendu :** Dossier `data/processed/test_output` créé avec fichiers Parquet.

---

### TC-INT-005 | Réconciliation des totaux

| Champ         | Valeur |
|---------------|--------|
| **Type**      | Integration |
| **Priorité**  | Critical |
| **Composant** | Data Quality |

**Résultat attendu :** Somme source ≈ somme target (tolérance 1%).
