//
//  RecordComparator.cpp
//  DBIp22final
//
//  Created by Dhananjay Sarsonia on 3/3/20.
//  Copyright © 2020 Dhananjay Sarsonia. All rights reserved.
//

#include "RecordComparator.h"
RecordComparator :: RecordComparator (OrderMaker *order) {
    
    sortorder = order;
    
    
}

bool RecordComparator::operator() (Record* left, Record* right) {
    
    ComparisonEngine engine;

    
    if (engine.Compare (left, right, sortorder) < 0) {
        
        return true;
        
    } else {
        
        return false;
        
    }
    
}
