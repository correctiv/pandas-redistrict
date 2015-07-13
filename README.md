# pandas-redistrict

Uses data on redistricting to apply redistricting to older datasets to represent the districts in their current state.

Supports merging and splitting of districts:
- Merged districts are summed up under new identifier
- Split districts are distributed by population-based ratio.

Data on redistricting is in `data/` directory. Currently only available for German *Kreise* (containing reforms in NRW, Sachsen, Sachsen-Anhalt and Mecklenburg-Vorpommern).

Install like this:

    pip install pandas-redistrict


## Example

``` python
>>> df  # Values of indexed by German district identifiers
value1  value2
AGS
05354       4       5
05313       5       6
05334       6       7
15154       8       9
15159      10      11
15151      12      13
15082      13      14

>>> # Port old identifiers to new versions. Sum and distribute values on the way
>>> from redistrict import redistrict
>>> redistrict(df, 'de/kreise', drop=True, splits=True)
value1  value2
AGS
05334   15.00   18.00
15001    2.40    2.60
15082   35.44   38.81
15086    0.96    1.04
15091    4.20    4.55
```
