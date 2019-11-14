package shardmaster

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

var server = []string{
	"192.168.1.1",
	"192.168.2.2",
	"192.168.3.3",
	"192.168.4.4",
}

type HashRing struct {
	replicateCount int // indicates how many virtual points should be used.
	nodes          map[uint32]string
	sortedNodes    []uint32
}

func (hr *HashRing) addNode(masterNode string) {

	for i := 0; i < hr.replicateCount; i++ {
		key := hr.hashKey(masterNode + strconv.Itoa(i) + strconv.Itoa(i))
		hr.nodes[key] = masterNode
		hr.sortedNodes = append(hr.sortedNodes, key)
	}

	sort.Slice(hr.sortedNodes, func(i, j int) bool {
		return hr.sortedNodes[i] < hr.sortedNodes[j]
	})
}

func (hr *HashRing) addNodes(masterNodes []string) {
	if len(masterNodes) > 0 {
		for _, node := range masterNodes {
			hr.addNode(node)
		}
	}
}

func (hr *HashRing) removeNode(masterNode string) {

	for i := 0; i < hr.replicateCount; {
		key := hr.hashKey(masterNode + strconv.Itoa(i) + strconv.Itoa(i))
		delete(hr.nodes, key)

		if success, index := hr.getIndexForKey(key); success {
			hr.sortedNodes = append(hr.sortedNodes[:index], hr.sortedNodes[index+1:]...)
		}
	}
}

func (hr *HashRing) getNode(key string) string {

	if len(hr.nodes) == 0 {
		return ""
	}

	hashKey := hr.hashKey(key)
	nodes := hr.sortedNodes

	masterNode := hr.nodes[nodes[0]]

	for _, node := range nodes {
		if hashKey < node {
			masterNode = hr.nodes[node]
			break
		}
	}

	return masterNode
}

func (hr *HashRing) getIndexForKey(key uint32) (bool, int) {

	index := -1
	success := false

	for i, v := range hr.sortedNodes {
		if v == key {
			index = i
			success = true
			break
		}
	}

	return success, index
}

func (hr *HashRing) hashKey(key string) uint32 {
	scratch := []byte(key)
	return crc32.ChecksumIEEE(scratch)
}

func New(nodes []string, replicateCount int) *HashRing {

	hr := new(HashRing)
	hr.replicateCount = replicateCount
	hr.nodes = make(map[uint32]string)
	hr.sortedNodes = []uint32{}
	hr.addNodes(nodes)

	return hr
}

func main() {
	hr := New(server, 10)

	//hr.addNode("192.168.5.5")
	fifth := 0
	first, second, third, four := 0, 0, 0, 0
	for i := 0; i < 10; i++ {
		str := hr.getNode(strconv.Itoa(i))
		if strings.Compare(str, "192.168.1.1") == 0 {
			fmt.Printf("192.168.1.1：%v \n", i)
			first++
		} else if strings.Compare(str, "192.168.2.2") == 0 {
			fmt.Printf("192.168.2.2：%v \n", i)
			second++
		} else if strings.Compare(str, "192.168.3.3") == 0 {
			fmt.Printf("192.168.3.3：%v \n", i)
			third++
		} else if strings.Compare(str, "192.168.4.4") == 0 {
			fmt.Printf("192.168.4.4：%v \n", i)
			four++
		} else if strings.Compare(str, "192.168.5.5") == 0 {
			fmt.Printf("192.168.5.5：%v \n", i)
			fifth++
		}
	}

	fmt.Printf("%v %v %v %v %v", first, second, third, four, fifth)

}
