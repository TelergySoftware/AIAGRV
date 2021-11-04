#! /usr/bin/awk -f

{
    n = split($2, a, ",")
    asort(a)
    printf a[1]
    for (i = 2; i <= n; ++i)
        printf "," a[i]
    n = split($3, a, ",")
    asort(a)
    printf " " a[1]
    for (i = 2; i <= n; ++i)
        printf "," a[i]
    print "" }
