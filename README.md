# pandas-redistrict

Uses data on redistricting to apply redistricting to older datasets to represent the districts in their current state.

Supports merging and splitting of districts:
- Merged districts are summed up under new identifier
- Split districts are distributed by population-based ratio.

Data on redistricting is in `data/` directory. Currently only available for German *Kreise* (containing reforms in NRW, Sachsen, Sachsen-Anhalt and Mecklenburg-Vorpommern).

Install like this:

    pip install pandas-redistrict


## Usage

``` python
>>> df  # Values indexed by German district identifiers
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

When you want to preserve groups inside districts, you can use ``redistrict_grouped``:

``` python
>>> # Specify district column (e.g. AGS)
>>> # Also specify groups to preserve, in this case year
>>> df
     AGS  year  value1  value2
0  05354  2008       4       5
1  05313  2008       5       6
2  05334  2011       6       7
3  15154  2005       8       9
4  15159  2005      10      11
5  15151  2005      12      13
6  15082  2013      13      14
>>> from redistrict import redistrict_grouped
    redistrict_grouped(df, 'de/kreise', ['year'],
                                    district_col='AGS',
                                    value_cols=['value1', 'value2'],
                                    drop=True)

     AGS  value1  value2  year
0  15001    2.40    2.60  2005
1  15082   22.44   24.81  2005
2  15086    0.96    1.04  2005
3  15091    4.20    4.55  2005
0  05334    9.00   11.00  2008
0  05334    6.00    7.00  2011
0  15082   13.00   14.00  2013
```
