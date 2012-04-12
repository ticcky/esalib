// bits.h
// Reseni IJC-DU1, priklad a), 12.3.2008
// Autor: Lukas Zilka(xzilka07), FIT 
// Prelozeno gcc 4.2 
//  
//

#ifndef _BITS_H
#define _BITS_H

//definice typu pole
#define LONG long

//zjistuje velikost pole
#define ArraySize(arr) (sizeof(arr)/sizeof(LONG))
#define ArrElementSize (sizeof(LONG)*8)

//vytvori promenou s polem arr o celkove velikosti min. size bitu
#define BitArray(arr,size)  LONG arr[(size/ArrElementSize)+1] = {0}
#define BitArrayDynamic(arr,size)  LONG* arr = calloc(1, ((size/ArrElementSize)+1) * sizeof(LONG));

//nastavuje bit ndx v bit.poli arr na hodnotu val
//#define SET_BIT(arr,ndx,val) ( arr[ndx/ArrElementSize] = ( val == 1 ? arr[ndx/ArrElementSize] | 1 << ndx % ArrElementSize : ~( ~arr[ndx/ArrElementSize] | 1 << ndx % ArrElementSize ) ) )
#define SET_BIT_DO(arr,ndx,val) ( arr[ndx/ArrElementSize] = ( val == 1 ? arr[ndx/ArrElementSize] | 1 << ndx % ArrElementSize : arr[ndx/ArrElementSize] & ( (~0) ^ (1 << ndx % ArrElementSize) ) ) )

#define SET_BIT(arr,ndx,val) (ArraySize(arr) > ( ndx/ArrElementSize  ) ?  SET_BIT_DO(arr,ndx,val) : (0) )

//zjistuje hodnotu bitu z bit.pole arr na pozici ndx
#define GET_BIT_DO(arr,ndx) ( ( (arr[ndx/ArrElementSize]) & (1 << ndx % ArrElementSize) ) == (1 << ndx % ArrElementSize) )
#define GET_BIT(arr,ndx) (ArraySize(arr) > ( ndx/ArrElementSize  ) ? GET_BIT_DO(arr,ndx) : (0) )


//pokud nechcem bitove inline fce pouzijem nasledujici obalujici makra pro nastavovani/zjistovani bitu
#ifndef USE_INLINE
#define SetBit(arr,ndx,val) (ArraySize(arr) > ( ndx/ArrElementSize  ) ?  SET_BIT(arr,ndx,val) : (0) )
#define GetBit(arr,ndx) GET_BIT(arr,ndx) 
#endif

//pokud chceme bitove inline fce pouzijem obalovaci inline fce pro praci s bity
#ifdef USE_INLINE
inline void SetBit( LONG arr[], int ndx, int val) {
    SET_BIT(arr,ndx,val);
    
}
inline int GetBit( LONG arr[], int ndx) {
    return GET_BIT(arr,ndx);
}
#endif

#endif
