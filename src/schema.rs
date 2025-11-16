// @generated automatically by Diesel CLI.

diesel::table! {
    blocks (hash) {
        chain_id -> Int8,
        creation_time -> Timestamptz,
        hash -> Varchar,
        height -> Int8,
        parent -> Varchar,
    }
}

diesel::table! {
    events (block, idx, request_key) {
        block -> Varchar,
        chain_id -> Int8,
        height -> Int8,
        idx -> Int8,
        module -> Varchar,
        module_hash -> Varchar,
        name -> Varchar,
        params -> Jsonb,
        param_text -> Varchar,
        qual_name -> Varchar,
        request_key -> Varchar,
        pact_id -> Nullable<Varchar>,
    }
}

diesel::table! {
    transactions (block, request_key) {
        bad_result -> Nullable<Jsonb>,
        block -> Varchar,
        chain_id -> Int8,
        code -> Nullable<Varchar>,
        continuation -> Nullable<Jsonb>,
        creation_time -> Timestamptz,
        data -> Nullable<Jsonb>,
        gas -> Int8,
        gas_limit -> Int8,
        gas_price -> Float8,
        good_result -> Nullable<Jsonb>,
        height -> Int8,
        logs -> Nullable<Varchar>,
        metadata -> Nullable<Jsonb>,
        nonce -> Varchar,
        num_events -> Nullable<Int8>,
        pact_id -> Nullable<Varchar>,
        proof -> Nullable<Varchar>,
        request_key -> Varchar,
        rollback -> Nullable<Bool>,
        sender -> Varchar,
        step -> Nullable<Int8>,
        ttl -> Int8,
        tx_id -> Nullable<Int8>,
    }
}

diesel::table! {
    transfers (block, chain_id, idx, module_hash, request_key) {
        amount -> Numeric,
        block -> Varchar,
        chain_id -> Int8,
        from_account -> Varchar,
        height -> Int8,
        idx -> Int8,
        module_hash -> Varchar,
        module_name -> Varchar,
        request_key -> Varchar,
        to_account -> Varchar,
        pact_id -> Nullable<Varchar>,
        creation_time -> Timestamptz,
    }
}

diesel::joinable!(events -> blocks (block));
diesel::joinable!(transactions -> blocks (block));
diesel::joinable!(transfers -> blocks (block));

diesel::allow_tables_to_appear_in_same_query!(
    blocks,
    events,
    transactions,
    transfers,
);
