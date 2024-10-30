export const DATABASE_TYPES = {
    HOTEL: "hotel",
    STORE: "store",
    HEALTHCARE: "healthcare",
    MUSIC_FESTIVAL: "music_festival",
    FORMULA_1: "formula_1"
};

export const ALLOWED_DATABASE_TYPES = {
    [DATABASE_TYPES.HOTEL]: 'Hotel (4 Tables)',
    [DATABASE_TYPES.STORE]: 'Store (6 Tables)',
    [DATABASE_TYPES.HEALTHCARE]: 'Healthcare (8 Tables)',
    [DATABASE_TYPES.MUSIC_FESTIVAL]: 'Music Festival (10 Tables)',
    [DATABASE_TYPES.FORMULA_1]: 'Formula 1 (14 Tables)'
};