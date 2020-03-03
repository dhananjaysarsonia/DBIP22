//
//  RunComparator.cpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "RunComparator.h"


bool RunComparator :: operator() (Run* left, Run* right) {
    
    ComparisonEngine engine;
    
    if (engine.Compare (left->currentRecord, right->currentRecord, left->sortedOrder) < 0) {
        
        return false;
        
    } else {
        
        return true;
        
    }
    
}
