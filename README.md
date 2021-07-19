# ANP Fuel Sales ETL Test - Raizen

## Goal

The developed pipeline is meant to extract and structure the underlying data of two of these tables:

- Sales of oil derivative fuels by UF and product
- Sales of diesel by UF and type

The totals of the extracted data must be equal to the totals of the pivot tables.

## Schema

Data should be stored in the following format:

| Column       | Type        |
| ------------ | ----------- |
| `year_month` | `date`      |
| `uf`         | `string`    |
| `product`    | `string`    |
| `unit`       | `string`    |
| `volume`     | `double`    |
| `created_at` | `timestamp` |

## Final considerations

I feel that I have evolved a lot in the development of this project, learning new ways to solve problems. Thanks for the opportunity.
