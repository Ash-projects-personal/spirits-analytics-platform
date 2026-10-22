# How this works, in plain words

## Why a star schema

Analytics questions here are all of one shape: slice sales by
something. By category, by channel, by month, by supplier. A star
schema puts the numbers in one centre table (fact_sales) and the
things you slice by in small tables around it (items, time). Every
query becomes a join and a group by, which is exactly what the
analysis code does. The schema was the honest core of the old
project too. What changed is the data feeding it.

## Retail, warehouse, transfers

Montgomery County sells alcohol two ways. Its own stores sell to
the public: that is retail. Its warehouse sells to licensed bars,
restaurants and shops: that is warehouse, and for beer it is most
of the volume. Transfers move stock between county stores. They
look like movement but nobody bought anything, so they are stored
in the fact table with their own column and never added to sales.

## REF and DUNNAGE

Real data has rows a simulator would never invent. REF rows are
returns, with negative unit counts. DUNNAGE is the deposit
container stream, also negative in this file. If I had summed the
whole file blindly, those adjustments would have quietly shrunk
the beer and wine totals. Instead they are loaded, labelled, kept
out of sales totals, and reported on their own line in
metrics.json. Finding and handling rows like these is most of
what real ETL work is.

## Why there is no margin number

The old summary promised a margin improvement because the string
was in the code, not because anything was measured. This dataset
has no money fields at all, so there is no honest margin to
compute. When a dataset cannot answer a question, the correct
output is to say so. If I later join this with a price list, the
margin analysis can come back, computed, with the join documented.
