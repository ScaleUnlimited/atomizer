package com.scaleunlimited.atomizer.flow;

import cascading.pipe.SubAssembly;

//Takes in tuples with atom, count (from Atomizer subassembly)
//
//Sorts atoms by count, and generates atom, atomId pairs.
//
//Question - do atomIds needs to provide rank info? I.e. if atomId1 < atomId2, then should atomId1's count > atomId2's count? If this isn't true, then our output would need to include count, for the KnotGen subassembly to be able to bring into memory (for the HashJoin) only the top N atom->atomId mappings.
//
//Question - how many unique atom values are you expecting? If the above question's answer is yes, then running things in a single reducer is the easiest way to get a global sorted count, for assigning atomIds in rank order.

public class AnchorGen extends SubAssembly {

}
