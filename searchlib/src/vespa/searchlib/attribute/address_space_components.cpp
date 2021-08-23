// Copyright Verizon Media. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "address_space_components.h"
#include "i_enum_store.h"

namespace search {

using vespalib::AddressSpace;

AddressSpace AddressSpaceComponents::default_enum_store_usage() {
    return AddressSpace(0, 0, IEnumStore::InternalIndex::offsetSize());
}

AddressSpace AddressSpaceComponents::default_multi_value_usage() {
    return AddressSpace(0, 0, (1ull << 32));
}

const vespalib::string AddressSpaceComponents::enum_store = "enum-store";
const vespalib::string AddressSpaceComponents::multi_value = "multi-value";
const vespalib::string AddressSpaceComponents::tensor_store = "tensor-store";
const vespalib::string AddressSpaceComponents::hnsw_node_store = "hnsw-node-store";
const vespalib::string AddressSpaceComponents::hnsw_link_store = "hnsw-link-store";

}
