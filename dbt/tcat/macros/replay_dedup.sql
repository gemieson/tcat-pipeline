{#
  without_replays(): the raw rows of a source, minus exact replays.

  A replay is one Kafka message written to GCS twice. The consumer uploaded a
  batch, stopped before committing, and re-read it on restart; or the producer
  retried a send whose acknowledgement was lost. Every envelope field is
  identical, so a replay is matched on all of them (replay_key).

  Not replays, and kept: the same GPS fix in a later poll (its ingested_at
  differs; int_vehicle_positions_deduped collapses those), and the same trip
  on many departure boards (that is what polling is for).

  dt and hr are left out of the key on purpose: a replay usually lands in a
  later file, so its partition can differ. The earliest copy is kept.

  Staging is rebuilt in full each run, so every replay is always compared with
  its original, whatever day it landed. Revisit (incremental staging) when raw
  data passes ~30 GiB: see docs/DECISIONS.md, D4.
#}
{% macro without_replays(source_relation, envelope_columns) -%}
    select * from (
        select
            *,
            farm_fingerprint(to_json_string(struct({{ envelope_columns | join(', ') }}))) as replay_key
        from {{ source_relation }}
    )
    where true   -- BigQuery wants a WHERE alongside QUALIFY
    qualify row_number() over (partition by replay_key order by dt, hr) = 1
{%- endmacro %}
