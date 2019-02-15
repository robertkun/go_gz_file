package main

import (
	"fmt"
	"os"
	"bufio"
	"time"
	"compress/gzip"
	"io"
	"flag"
	"path/filepath"
)

var bufsizes = []int{
	0, 16, 23, 32, 46, 64, 93, 128, 1024, 4096,
}

// 判断文件是否存在
func FileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func MakeDir(path string) error {
	bExist, err := PathExists(path)
	if !bExist && err == nil {
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			fmt.Printf("mkdir failed![%v]\n", err)
			return err
		}
	}

	return nil
}

func main() {
	bln := flag.Bool("l", false, "按行模式处理, 默认:按块模式处理")
	bgz := flag.Bool("g", false, "读取压缩格式文件, 默认:false")
	nsp := flag.Int("s", 0, "每次处理sleep毫秒数, 默认:0毫秒")
	nblk := flag.Int("b", 1024, "文件读取块大小, 默认1024KB")
	fin := flag.String("i", "", "输入文件")
	fout := flag.String("o", "", "输出文件")
	flag.Parse()

	if *fin == "" {
		fmt.Println("输入文件为空! 返回!")
		return
	}

	if *fout == "" {
		fmt.Println("输出文件为空! 返回!")
		return
	}

	nSleep := *nsp
	nBlock := *nblk
	fn_in := *fin
	fn_out := *fout
	bline := *bln
	bgzfile := *bgz

	fmt.Printf("输入文件:%v\n", fn_in)
	fmt.Printf("输出文件:%v\n", fn_out)
	fmt.Printf("处理间隔:%v毫秒\n", nSleep)
	fmt.Printf("按行处理:%v\n", bline)
	fmt.Printf("读取块大小:%v\n", nBlock)
	fmt.Printf("是否处理压缩格式文件:%v\n", bgzfile)
	fmt.Println("\n--- 开始处理 ---")

	if bline {
		AppendByLine(fn_in, fn_out, nSleep, bgzfile)
	} else {
		AppendByBlock(fn_in, fn_out, nSleep, nBlock, bgzfile)
	}
}

func AppendByLine(fn_in, fn_out string, nRate int, bgzfile bool) {
	start := time.Now()
	paths, _ := filepath.Split(fn_in)
	MakeDir(paths)

	_, err := os.Stat(fn_out)
	if err != nil {
		fmt.Println("获取文件状态失败! err:", err)
		return
	}

	bExistd, err := FileExists(fn_out)
	if err != nil {
		return
	}

	if !bExistd {
		fmt.Printf("文件:%v不存在!\n", fn_out)
		return
	}

	/*-----------------------------------------------------
	打开待处理的gz格式文件
	-----------------------------------------------------*/
	fr_in, err := os.OpenFile(fn_in, os.O_RDWR|os.O_CREATE|os.O_APPEND,644)
	if err != nil {
		fmt.Println("-------- file open failed!", fn_in)
		return
	}

	defer fr_in.Close()

	// 创建gzip文件
	gr_in := gzip.NewWriter(fr_in)
	defer gr_in.Close()

	/*-----------------------------------------------------
	打开待读取gz格式文件
	-----------------------------------------------------*/
	fr_out, err := os.Open(fn_out)
	if err != nil {
		fmt.Println("-------- file open failed!", fn_out)
		return
	}

	defer fr_out.Close()

	buf_out := bufio.NewReader(fr_out)
	if bgzfile {
		// 创建gzip文件读取对象
		gr_out, err := gzip.NewReader(fr_out)
		if err != nil {
			fmt.Println("-------- file read failed!", fn_out)
			return
		}

		defer gr_out.Close()
		buf_out = bufio.NewReader(gr_out)
	}

	line := 0
	for {
		strline, err := buf_out.ReadString('\n')
		line++
		if err != nil {
			if err == io.EOF {
				//fmt.Printf("file:%v, io.EOF! error! Err:%v\n", fn, err)
				break
			} else {
				fmt.Printf("file:%v, read file error! Err:%v\n", fn_out, err)
				break
			}
		}

		n, err := gr_in.Write([]byte(strline))
		if err := gr_in.Flush(); err != nil {
			fmt.Println(n, err)
		}

		if nRate != 0 {
			time.Sleep(time.Duration(nRate) * time.Millisecond)
		}

		if line%10000 == 0 {
			fmt.Printf("--- 已处理%v行\n", line)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("--- 文件:%v处理完成! 共处理:%v行, 耗时:%vms!\n", fn_out, line, elapsed.Nanoseconds()/1e6)
}

func AppendByBlock(fn_in, fn_out string, nRate, nBlock int, bgzfile bool) {
	start := time.Now()
	paths, _ := filepath.Split(fn_in)
	MakeDir(paths)

	_, err := os.Stat(fn_out)
	if err != nil {
		fmt.Println("获取文件状态失败! err:", err)
		return
	}

	bExistd, err := FileExists(fn_out)
	if err != nil {
		return
	}

	if !bExistd {
		fmt.Printf("文件:%v不存在!\n", fn_out)
		return
	}

	/*-----------------------------------------------------
	打开待处理的gz格式文件
	-----------------------------------------------------*/
	fr_in, err := os.OpenFile(fn_in, os.O_RDWR|os.O_CREATE|os.O_APPEND,644)
	if err != nil {
		fmt.Println("-------- file open failed!", fn_in)
		return
	}

	defer fr_in.Close()

	// 创建gzip文件
	gr_in := gzip.NewWriter(fr_in)
	defer gr_in.Close()

	/*-----------------------------------------------------
	打开待读取gz格式文件
	-----------------------------------------------------*/
	fr_out, err := os.Open(fn_out)
	if err != nil {
		fmt.Println("-------- file open failed!", fn_out)
		return
	}

	defer fr_out.Close()

	buf_out := bufio.NewReader(fr_out)
	if bgzfile {
		// 创建gzip文件读取对象
		gr_out, err := gzip.NewReader(fr_out)
		if err != nil {
			fmt.Println("-------- err: file read failed!", fn_out, err)
			return
		}

		defer gr_out.Close()
		buf_out = bufio.NewReader(gr_out)
	}

	line := 0
	for {
		buf := make([]byte, nBlock)
		nr, err := buf_out.Read(buf)
		if err != nil {
			if err == io.EOF {
				//fmt.Printf("file:%v, io.EOF! error! Err:%v\n", fn, err)
				break
			} else {
				fmt.Printf("file:%v, read file error! Err:%v\n", fn_out, err)
				break
			}
		}

		if nr == 0 {
			break
		}

		line++
		nw, err := gr_in.Write(buf[:nr])
		if err := gr_in.Flush(); err != nil {
			fmt.Println("--- err: read size, write size=", nr, nw, err)
		}

		if nr != nw {
			fmt.Println("--- err: read size, write size=", nr, nw)
		}

		if nRate != 0 {
			time.Sleep(time.Duration(nRate) * time.Millisecond)
		}

		if line%100 == 0 {
			fmt.Printf("--- 已处理%v次\n", line)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("--- 文件:%v处理完成! 共处理:%v次, 耗时:%vms!\n", fn_out, line, elapsed.Nanoseconds()/1e6)
}