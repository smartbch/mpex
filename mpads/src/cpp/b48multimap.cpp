#include <stdint.h>
#include "cpp-btree-1.0.1/btree_map.h"

extern "C" {
	size_t cppbtree_new();
	void cppbtree_delete(size_t tree);
	size_t cppbtree_size(size_t tree);

	bool cppbtree_insert(size_t tree, uint64_t key, int64_t value);
	bool cppbtree_erase(size_t tree, uint64_t key, int64_t value);
	bool cppbtree_change(size_t tree, uint64_t key, int64_t old_v, int64_t new_v);

	size_t cppbtree_seek(size_t tree, uint64_t key);

	void iter_next(size_t iter);
	void iter_prev(size_t iter);
	bool is_first(size_t tree, size_t iter);
	bool iter_valid(size_t tree, size_t iter);
	uint64_t iter_key(size_t iter);
	int64_t iter_value(size_t iter);
	void iter_delete(size_t iter);
}

//compact integer whose alignment=1
//we prefer this class because int32 and int64 will cause padding because of alignment
template<int byte_count>
struct bits_n {
	uint8_t b[byte_count];
	static bits_n from_uint64(uint64_t uint64) {
		bits_n v;
		for(int i=0; i<byte_count; i++) { //little endian
			v.b[i] = uint8_t(uint64>>(i*8));
		}
		return v;
	}
	static bits_n from_int64(int64_t int64) {
		return from_uint64(uint64_t(int64));
	}
	static bits_n from_uint32(uint32_t uint32) {
		return from_uint64(uint64_t(uint32));
	}
	uint64_t to_uint64() const {
		uint64_t v = 0;
		for(int i=byte_count-1; i>=0; i--) { //little endian
			v = (v<<8) | uint64_t(b[i]);
		}
		return v;
	}
	int64_t to_int64() const {
		return int64_t(to_uint64());
	}
	uint32_t to_uint32() const {
		return uint32_t(to_uint64());
	}
	bool operator<(const bits_n& other) const {
		return this->to_uint64() < other.to_uint64();
	}
	bool operator==(const bits_n& other) const {
		return this->to_uint64() == other.to_uint64();
	}
	bool operator!=(const bits_n& other) const {
		return this->to_uint64() != other.to_uint64();
	}
};

typedef bits_n<6> bits48;

typedef btree::btree_multimap<bits48, bits48> basic_map;

size_t cppbtree_new() {
	basic_map* m = new basic_map;
	return (size_t)m;
}

void cppbtree_delete(size_t tree) {
	basic_map* m = (basic_map*)tree;
	delete m;
}

size_t cppbtree_size(size_t tree) {
	basic_map* m = (basic_map*)tree;
	return m->size();
}

bool cppbtree_insert(size_t tree, uint64_t key, int64_t value) {
	basic_map* m = (basic_map*)tree;
	bits48 k = bits48::from_uint64(key);
	bits48 v = bits48::from_int64(value);

	basic_map::iterator it = m->lower_bound(k);
	while(it != m->end() && it->first.to_uint64() == key) {
		if(it->second.to_int64() == value) {
			return false;
		}
		it++;
	}
	m->insert(it, std::make_pair(k, v));
	return true;
}

bool cppbtree_erase(size_t tree, uint64_t key, int64_t value) {
	basic_map* m = (basic_map*)tree;
	bits48 k = bits48::from_uint64(key);
	basic_map::iterator it = m->lower_bound(k);
	bool key_match = false;
	bool value_match = false;
	while(true) {
		key_match = it->first.to_uint64() == key;
		value_match = it->second.to_int64() == value;
		if(key_match && !value_match) {
			it++;
		} else {
			break;
		}
	}
	if(key_match && value_match) {
		m->erase(it);
		return true;
	}
	return false;
}

bool cppbtree_change(size_t tree, uint64_t key, int64_t old_v, int64_t new_v) {
	basic_map* m = (basic_map*)tree;
	bits48 k = bits48::from_uint64(key);
	basic_map::iterator it = m->lower_bound(k);
	bool key_match = false;
	bool value_match = false;
	while(true) {
		key_match = it->first.to_uint64() == key;
		value_match = it->second.to_int64() == old_v;
		if(key_match && !value_match) {
			it++;
		} else {
			break;
		}
	}
	if(key_match && value_match) {
		it->second = bits48::from_int64(new_v);
		return true;
	}
	return false;
}

size_t cppbtree_seek(size_t tree, uint64_t key) {
	basic_map* m = (basic_map*)tree;
	bits48 k = bits48::from_uint64(key);
	basic_map::iterator* it = new basic_map::iterator;
	*it = m->lower_bound(k);
	return (size_t)it;
}

void iter_next(size_t iter) {
	basic_map::iterator& it = *((basic_map::iterator*)iter);
	it++;
}

void iter_prev(size_t iter) {
	basic_map::iterator& it = *((basic_map::iterator*)iter);
	it--;
}

bool is_first(size_t tree, size_t iter) {
	basic_map::iterator& it = *((basic_map::iterator*)iter);
	basic_map* m = (basic_map*)tree;
	return m->begin() == it;
}

bool iter_valid(size_t tree, size_t iter) {
	basic_map::iterator& it = *((basic_map::iterator*)iter);
	basic_map* m = (basic_map*)tree;
	return m->end() != it;
}

uint64_t iter_key(size_t iter) {
	basic_map::iterator& it = *((basic_map::iterator*)iter);
	return it->first.to_uint64();
}

int64_t iter_value(size_t iter) {
	basic_map::iterator& it = *((basic_map::iterator*)iter);
	return it->second.to_int64();
}

void iter_delete(size_t iter) {
	basic_map::iterator* it = (basic_map::iterator*)iter;
	delete it;
}
