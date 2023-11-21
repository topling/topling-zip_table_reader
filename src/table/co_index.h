#pragma once

#include <terark/fstring.hpp>
#include <terark/valvec.hpp>
#include <terark/util/refcount.hpp>
#include <terark/int_vector.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/noncopyable.hpp>
#include <topling/json_fwd.h>
#include <memory>

namespace rocksdb {

using terark::fstring;
using terark::valvec;
using terark::byte_t;
using std::unique_ptr;
using nlohmann::json;

struct ToplingZipTableOptions;
struct ImmutableOptions;
class COIndex;
class COIndexDel {
public:
  void operator()(COIndex* ptr) const;
};
using COIndexUP = std::unique_ptr<COIndex, COIndexDel>;

class COIndex : boost::noncopyable {
public:
  size_t m_num_keys = size_t(-1);
public:
  class Iterator : boost::noncopyable {
  protected:
    size_t m_id = size_t(-1);
    virtual ~Iterator();
  public:
    virtual void Delete();
    virtual bool SeekToFirst() = 0;
    virtual bool SeekToLast() = 0;
    virtual bool Seek(fstring target) = 0;
    virtual bool Next() = 0;
    virtual bool Prev() = 0;
    virtual size_t DictRank() const = 0;
    inline bool Valid() const noexcept { return size_t(-1) != m_id; }
    inline size_t id() const noexcept { return m_id; }
    inline fstring key() const noexcept;
    inline void SetInvalid() noexcept { m_id = size_t(-1); }
  };
  class FastIter : public Iterator {
  public:
    size_t  m_num;
    fstring m_key;
  };
  struct KeyStat {
    size_t minKeyLen = size_t(-1);
    size_t maxKeyLen = 0;
    size_t sumKeyLen = 0;
    size_t numKeys   = 0;
    size_t holeLen   = 0;
    valvec<byte_t> minKey;
    valvec<byte_t> maxKey;
    valvec<byte_t> prefix; // fixed-len prefix
    valvec<uint16_t> holeMeta;

    // will not be non-empty both
    terark::UintVecMin0 keyOffsets; // if non-empty, Build() will use it
    bool hasFastApproximateRank = true;
    bool IsFixedLen() const { return minKeyLen == maxKeyLen; }
  };
  class Factory : public terark::RefCounter {
  public:
    size_t  mapIndex = size_t(-1);
    virtual ~Factory();
    virtual COIndex* Build(const std::string& keyFilePath,
                               const ToplingZipTableOptions& tzopt,
                               const KeyStat&,
                               const ImmutableOptions* ioption = nullptr) const = 0;
    virtual COIndexUP LoadMemory(fstring mem) const = 0;
    virtual COIndexUP LoadFile(fstring fpath) const = 0;
    virtual size_t MemSizeForBuild(const KeyStat&) const = 0;
    const char* WireName() const;
  };
  typedef boost::intrusive_ptr<Factory> FactoryPtr;
  struct AutoRegisterFactory {
    AutoRegisterFactory(std::initializer_list<const char*> names,
        const char* rtti_name, Factory* factory);
  };
  static const Factory* GetFactory(fstring name);
  static const Factory* SelectFactory(const KeyStat&, fstring name);
  static bool SeekCostEffectiveIndexLen(const KeyStat& ks, size_t& ceLen);
  static COIndexUP LoadFile(fstring fpath);
  static COIndexUP LoadMemory(fstring mem);
  static COIndexUP LoadMemory(const void* mem, size_t len) {
    return LoadMemory(fstring((const char*)mem, len));
  }
protected:
  virtual ~COIndex();
public:
  virtual void Delete();
  virtual const char* Name() const = 0;
  virtual void SaveMmap(std::function<void(const void *, size_t)> write) const = 0;
  virtual size_t Find(fstring key) const = 0;
  virtual size_t AccurateRank(fstring key) const = 0;
  virtual size_t ApproximateRank(fstring key) const = 0;
  inline  size_t NumKeys() const { return m_num_keys; }
  virtual size_t TotalKeySize() const = 0;
  virtual std::string NthKey(size_t nth) const = 0;
  virtual fstring Memory() const = 0;
  virtual Iterator* NewIterator() const = 0;
  virtual bool NeedsReorder() const = 0;
  virtual void GetOrderMap(terark::UintVecMin0& newToOld) const = 0;
  virtual void BuildCache(double cacheRatio) = 0;
  virtual json MetaToJson() const = 0;
  virtual bool HasFastApproximateRank() const noexcept;
  virtual fstring ResidentMemory() const noexcept;
  void VerifyDictRank() const;
};

inline fstring COIndex::Iterator::key() const noexcept {
  assert(m_id != size_t(-1));
  return static_cast<const FastIter*>(this)->m_key;
}

#define COIndexRegister(clazz, ...) \
    COIndexRegisterImp(clazz, #clazz, ##__VA_ARGS__)

#define COIndexRegisterImp(clazz, WireName, ...) \
    BOOST_STATIC_ASSERT(sizeof(WireName) <= 60); \
    COIndex::AutoRegisterFactory                 \
    terark_used_static_obj                       \
    g_AutoRegister_##clazz(                      \
        {WireName,__VA_ARGS__},                  \
        typeid(clazz).name(),                    \
        new clazz::MyFactory()                   \
    )

}

