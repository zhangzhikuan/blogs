## hadoop创建FileSystem

```
val path = new Path("hdfs://mls/tmp/")
path.getFileSystem(conf)
```

#### Path
```
  /** Return the FileSystem that owns this Path. */
  public FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(this.toUri(), conf);
  }
```

#### FileSystem
```
  /** Returns the FileSystem for this URI's scheme and authority.  The scheme
   * of the URI determines a configuration property name,
   * <tt>fs.<i>scheme</i>.class</tt> whose value names the FileSystem class.
   * The entire URI is passed to the FileSystem instance's initialize method.
   */
  public static FileSystem get(URI uri, Configuration conf) throws IOException {
    String scheme = uri.getScheme();
    String authority = uri.getAuthority();

    if (scheme == null && authority == null) {     // use default FS
      return get(conf);
    }

    if (scheme != null && authority == null) {     // no authority
      URI defaultUri = getDefaultUri(conf);
      if (scheme.equals(defaultUri.getScheme())    // if scheme matches default
          && defaultUri.getAuthority() != null) {  // & default has authority
        return get(defaultUri, conf);              // return default
      }
    }
    
    String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
    if (conf.getBoolean(disableCacheName, false)) {
      return createFileSystem(uri, conf);
    }
	//从缓存中读取
    return CACHE.get(uri, conf);
  }

	//第二步
    FileSystem get(URI uri, Configuration conf) throws IOException{
      Key key = new Key(uri, conf);
   		//继续
      return getInternal(uri, conf, key);
    }

	//第三步
 private FileSystem getInternal(URI uri, Configuration conf, Key key) throws IOException{
      FileSystem fs;
      synchronized (this) {
        fs = map.get(key);
      }
      if (fs != null) {
        return fs;
      }
		//开始创建fs
      fs = createFileSystem(uri, conf);
      synchronized (this) { // refetch the lock again
        FileSystem oldfs = map.get(key);
        if (oldfs != null) { // a file system is created while lock is releasing
          fs.close(); // close the new file system
          return oldfs;  // return the old file system
        }
        
        // now insert the new file system into the map
        if (map.isEmpty()
                && !ShutdownHookManager.get().isShutdownInProgress()) {
          ShutdownHookManager.get().addShutdownHook(clientFinalizer, SHUTDOWN_HOOK_PRIORITY);
        }
        fs.key = key;
        map.put(key, fs);
        if (conf.getBoolean("fs.automatic.close", true)) {
          toAutoClose.add(key);
        }
        return fs;
      }
    }

	//第四步
  private static FileSystem createFileSystem(URI uri, Configuration conf
      ) throws IOException {
    Class<?> clazz = getFileSystemClass(uri.getScheme(), conf);
    if (clazz == null) {
      throw new IOException("No FileSystem for scheme: " + uri.getScheme());
    }
    //反射得到FileSystem
    FileSystem fs = (FileSystem)ReflectionUtils.newInstance(clazz, conf);
    //初始化
    fs.initialize(uri, conf);
    return fs;
  }
```


#### DistributedFileSystem
```
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    setConf(conf);

    String host = uri.getHost();
    if (host == null) {
      throw new IOException("Incomplete HDFS URI, no host: "+ uri);
    }
    homeDirPrefix = conf.get(
        DFSConfigKeys.DFS_USER_HOME_DIR_PREFIX_KEY,
        DFSConfigKeys.DFS_USER_HOME_DIR_PREFIX_DEFAULT);
    
    //创建dfs client
    this.dfs = new DFSClient(uri, conf, statistics);
    this.uri = URI.create(uri.getScheme()+"://"+uri.getAuthority());
    this.workingDir = getHomeDirectory();
  }
```

#### DFSClient
```
    if (proxyInfo != null) {
      this.dtService = proxyInfo.getDelegationTokenService();
      this.namenode = proxyInfo.getProxy();
    } else if (rpcNamenode != null) {
      // This case is used for testing.
      Preconditions.checkArgument(nameNodeUri == null);
      this.namenode = rpcNamenode;
      dtService = null;
    } else {
      Preconditions.checkArgument(nameNodeUri != null,
          "null URI");
      //创建nameNode的代理
      proxyInfo = NameNodeProxies.createProxy(conf, nameNodeUri,
          ClientProtocol.class, nnFallbackToSimpleAuth);
      this.dtService = proxyInfo.getDelegationTokenService();
      this.namenode = proxyInfo.getProxy();
    }
```

#### NameNodeProxies
```
  public static <T> ProxyAndInfo<T> createProxy(Configuration conf,
      URI nameNodeUri, Class<T> xface, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
      
      //查找NNFailoverProxyProvider，一般像NameNode的HA，Provider是
      //org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
      //下一步，我们要看看如何查找匹配的NNFailoverProxyProvider
      //通过反射的来创建NNFailoverProxyProvider
      //最终运行到org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider的构造方法
    AbstractNNFailoverProxyProvider<T> failoverProxyProvider =
        createFailoverProxyProvider(conf, nameNodeUri, xface, true,
          fallbackToSimpleAuth);
  
  	//如果没有找到匹配的failoverProxyProvider的话，默认是直连，也就是说直接和NameNode的RPC Address连接。
    if (failoverProxyProvider == null) {
      // Non-HA case
      return createNonHAProxy(conf, NameNode.getAddress(nameNodeUri), xface,
          UserGroupInformation.getCurrentUser(), true, fallbackToSimpleAuth);
    } else {
      // HA case
      //如果找到匹配的failoverProxyProvider的话，通过创建的failoverProxyProvider的到封装好的代理对象
      Conf config = new Conf(conf);
      T proxy = (T) RetryProxy.create(xface, failoverProxyProvider,
          RetryPolicies.failoverOnNetworkException(
              RetryPolicies.TRY_ONCE_THEN_FAIL, config.maxFailoverAttempts,
              config.maxRetryAttempts, config.failoverSleepBaseMillis,
              config.failoverSleepMaxMillis));

      Text dtService;
      if (failoverProxyProvider.useLogicalURI()) {
        dtService = HAUtil.buildTokenServiceForLogicalUri(nameNodeUri,
            HdfsConstants.HDFS_URI_SCHEME);
      } else {
        dtService = SecurityUtil.buildTokenService(
            NameNode.getAddress(nameNodeUri));
      }
      return new ProxyAndInfo<T>(proxy, dtService,
          NameNode.getAddress(nameNodeUri));
    }
  }
```

#### ConfiguredFailoverProxyProvider
```

  public ConfiguredFailoverProxyProvider(Configuration conf, URI uri,
      Class<T> xface) {
    Preconditions.checkArgument(
        xface.isAssignableFrom(NamenodeProtocols.class),
        "Interface class %s is not a valid NameNode protocol!");
    this.xface = xface;
    
    this.conf = new Configuration(conf);
    int maxRetries = this.conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_KEY,
        DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
        maxRetries);
    
    int maxRetriesOnSocketTimeouts = this.conf.getInt(
        DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        DFSConfigKeys.DFS_CLIENT_FAILOVER_CONNECTION_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
    this.conf.setInt(
        CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        maxRetriesOnSocketTimeouts);
    
    try {
      ugi = UserGroupInformation.getCurrentUser();
      //此处是核心代码，查找所有可用的NameNode的RPC地址
      Map<String, Map<String, InetSocketAddress>> map = DFSUtil.getHaNnRpcAddresses(
          conf);
      //根据Ha的服务名获取所有的NameNode列表
      Map<String, InetSocketAddress> addressesInNN = map.get(uri.getHost());
      
      if (addressesInNN == null || addressesInNN.size() == 0) {
        throw new RuntimeException("Could not find any configured addresses " +
            "for URI " + uri);
      }
      
      Collection<InetSocketAddress> addressesOfNns = addressesInNN.values();
      for (InetSocketAddress address : addressesOfNns) {
        proxies.add(new AddressRpcProxyPair<T>(address));
      }

      // The client may have a delegation token set for the logical
      // URI of the cluster. Clone this token to apply to each of the
      // underlying IPC addresses so that the IPC code can find it.
      HAUtil.cloneDelegationTokenForLogicalUri(ugi, uri, addressesOfNns);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
```

---
#### 类名：DFSUtil

```
  /**
   * Returns list of InetSocketAddress corresponding to HA NN RPC addresses from
   * the configuration.
   * 
   * @param conf configuration
   * @return list of InetSocketAddresses
   */
  public static Map<String, Map<String, InetSocketAddress>> getHaNnRpcAddresses(
      Configuration conf) {
    return getAddresses(conf, null, DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY);
  }

  /**
   * Returns the configured address for all NameNodes in the cluster.
   * @param conf configuration
   * @param defaultAddress default address to return in case key is not found.
   * @param keys Set of keys to look for in the order of preference
   * @return a map(nameserviceId to map(namenodeId to InetSocketAddress))
   */
  private static Map<String, Map<String, InetSocketAddress>>
    getAddresses(Configuration conf, String defaultAddress, String... keys) {
    //获取所有的NameService，对应的参数是  ｀dfs.nameservices｀
    Collection<String> nameserviceIds = getNameServiceIds(conf);
    //开始查找对应NameService的NameNode id
    return getAddressesForNsIds(conf, nameserviceIds, defaultAddress, keys);
  }
  
   /**
   * Returns the configured address for all NameNodes in the cluster.
   * @param conf configuration
   * @param nsIds
   *@param defaultAddress default address to return in case key is not found.
   * @param keys Set of keys to look for in the order of preference   @return a map(nameserviceId to 					map(namenodeId to InetSocketAddress))
   */
  private static Map<String, Map<String, InetSocketAddress>>
    getAddressesForNsIds(Configuration conf, Collection<String> nsIds,
                         String defaultAddress, String... keys) {
    // Look for configurations of the form <key>[.<nameserviceId>][.<namenodeId>]
    // across all of the configured nameservices and namenodes.
    Map<String, Map<String, InetSocketAddress>> ret = Maps.newLinkedHashMap();
    for (String nsId : emptyAsSingletonNull(nsIds)) {
      Map<String, InetSocketAddress> isas =
      	//循环便利NameService，然后得到NameNode的RPC地址，并且返回，此处对应的参数是 `dfs.ha.namenodes.${NameSerivceId}`
        getAddressesForNameserviceId(conf, nsId, defaultAddress, keys);
      if (!isas.isEmpty()) {
        ret.put(nsId, isas);
      }
    }
    return ret;
  }
  
  //获取和所有的NameNode关联的地址
    private static Map<String, InetSocketAddress> getAddressesForNameserviceId(
      Configuration conf, String nsId, String defaultValue,
      String... keys) {
      //获得NameNode的列表
    Collection<String> nnIds = getNameNodeIds(conf, nsId);
    Map<String, InetSocketAddress> ret = Maps.newHashMap();
    for (String nnId : emptyAsSingletonNull(nnIds)) {
      String suffix = concatSuffixes(nsId, nnId);
      //获取RPC地址，读经的参数 ｀dfs.namenode.rpc-address.${NameServiceId}.${NameNodeId}`
      String address = getConfValue(defaultValue, suffix, conf, keys);
      if (address != null) {
      		//创建RPC地址
        InetSocketAddress isa = NetUtils.createSocketAddr(address);
        if (isa.isUnresolved()) {
          LOG.warn("Namenode for " + nsId +
                   " remains unresolved for ID " + nnId +
                   ".  Check your hdfs-site.xml file to " +
                   "ensure namenodes are configured properly.");
        }
        ret.put(nnId, isa);
      }
    }
    return ret;
  }

```
---
到此，如何和NameNode进行交互的流程已经梳理清楚了。用到了代理的模式



