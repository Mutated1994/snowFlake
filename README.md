## twitter SnowFlake算法

这个算法的生命周期差不多是68年，所以这个包最多可以使用到2087年7月21日

在使用过程中千万不要修改epoch的值... 千万不要  

### 参考代码（根据前辈的代码优化）
https://github.com/holdno/snowFlakeByGo
  
### 算法详解  
https://segmentfault.com/a/1190000014767902

### 如何使用 HOW TO USE  
``` golang
import "github.com/Mutated1994/snowFlake"

var IdWorker *snowFlake.Worker

func main(){
	// 生成一个节点实例
	IdWorker, _ = snowFlake.NewWorker(0) // 传入当前节点id 此id在机器集群中一定要唯一 且从0开始排最多1024个节点，可以根据节点的不同动态调整该算法每毫秒生成的id上限(如何调整会在后面讲到)  
	
	// 获得唯一id
	id := IdWorker.GetId()
	// 就是这么easy...
}
```

### 调整节点数量改变每毫秒生成上限  
``` golang
workerBits uint8 = 10 // 每台机器(节点)的ID位数 10位最大可以有2^10=1024个节点
numberBits uint8 = 12 // 表示每个集群下的每个节点，1毫秒内可生成的id序号的二进制位数 即每毫秒可生成 2^12-1=4096个唯一ID
// 可以在snowFlake.go中动态改变10 12这两个数值来动态改变，但总和不能被改变(10 + 12 = 22)
```
