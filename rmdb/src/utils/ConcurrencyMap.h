#ifndef CONCURRENCY_MAP_H
#define CONCURRENCY_MAP_H

#include "utils/log.h"
#include <mutex>
#include <unordered_map>

namespace utils {
	template<typename K, typename V, typename hash>
	class ConcurrencyMap {
	public:
		ConcurrencyMap() {}

		~ConcurrencyMap() {}

		ConcurrencyMap(const ConcurrencyMap &rhs) {
			map_ = rhs.map_;
		}

		ConcurrencyMap &operator=(const ConcurrencyMap &rhs) {
			if (&rhs != this) {
				map_ = rhs.map_;
			}
			return *this;
		}

		V &operator[](const K &key) {
			return map_[key];
		}

		// when multithread calling size() return a tmp status, some threads may insert just after size() call
		int Size() {
			std::lock_guard<std::mutex> lock(mutex_);
			return map_.size();
		}

		// when multithread calling Empty() return a tmp status, some threads may insert just after Empty() call
		bool IsEmpty() {
			std::lock_guard<std::mutex> lock(mutex_);
			return map_.empty();
		}

		bool Insert(const K &key, const V &value) {
			std::lock_guard<std::mutex> lock(mutex_);
			auto ret = map_.insert(std::pair<K, V>(key, value));
			return ret.second;
		}

		void EnsureInsert(const K &key, const V &value) {
			std::lock_guard<std::mutex> lock(mutex_);
			auto ret = map_.insert(std::pair<K, V>(key, value));
			// find key and cannot insert
			if (!ret.second) {
				map_.erase(ret.first);
				map_.insert(std::pair<K, V>(key, value));
				return;
			}
			return;
		}

		bool Find(const K &key, V &value) {
			bool ret = false;
			std::lock_guard<std::mutex> lock(mutex_);
			auto iter = map_.find(key);
			if (iter != map_.end()) {
				value = iter->second;
				ret = true;
			}
			return ret;
		}

		bool FindOldAndSetNew(const K &key, V &oldValue, const V &newValue) {
			bool ret = false;
			std::lock_guard<std::mutex> lock(mutex_);
			if (map_.size() > 0) {
				auto iter = map_.find(key);
				if (iter != map_.end()) {
					oldValue = iter->second;
					map_.erase(iter);
					map_.insert(std::pair<K, V>(key, newValue));
					ret = true;
				}
			}
			return ret;
		}

		void Erase(const K &key) {
			std::lock_guard<std::mutex> lock(mutex_);
			map_.erase(key);
		}

		void Clear() {
			std::lock_guard<std::mutex> lock(mutex_);
			map_.clear();
			return;
		}

	private:
		std::mutex mutex_;
		std::unordered_map<K, V, hash> map_;
	};
}// namespace utils
#endif