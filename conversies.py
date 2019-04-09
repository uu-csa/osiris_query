def rename_faculteiten(df):
    fac_name = {
        'RA': 'UCR',
        'UC': 'UCU',
        'IVLOS': 'GST',
    }
    df.loc[:, 'FACULTEIT'] = df['FACULTEIT'].replace(to_replace=fac_name)
    return df