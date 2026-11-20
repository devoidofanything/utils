package chans

import (
	"context"
	"time"
)

/*
       _                 _     _
    __| | _____   _____ (_) __| |
   / _` |/ _ \ \ / / _ \| |/ _` |
  | (_| |  __/\ V / (_) | | (_| |
   \__,_|\___| \_/ \___/|_|\__,_|
*/

type ShardChunk[T any] struct {
	sharder  *ShardChan[T]
	chunkers []*ChunkChan[T]
}

type ShardChunkFunc[T any] func(int, []T)

func (s *ShardChunk[T]) ShardCount() int {
	return s.sharder.ShardCount()
}

func (s *ShardChunk[T]) C(key int) chan []T {
	if key < 0 || key >= s.sharder.ShardCount() {
		return nil
	}
	return s.chunkers[key].C()
}

func (s *ShardChunk[T]) Sizes() ([]int, int, int) {
	shardSizes := s.sharder.Sizes()
	chunkerTotalSizeIn, chunkerTotalSizeOut := 0, 0
	for i := 0; i < s.sharder.ShardCount(); i++ {
		in, out := s.chunkers[i].Sizes()
		chunkerTotalSizeIn += in
		chunkerTotalSizeOut += out
	}
	return shardSizes, chunkerTotalSizeIn, chunkerTotalSizeOut
}

type ShardChunkBuilder[T any] struct {
	ctx                context.Context
	chunkSize          int
	chunkTimeout       time.Duration
	shardCount         int
	shardFunc          ShardFunc[T]
	shardWorker        ShardChunkFunc[T]
	incomingChan       chan T
	incomingBufferSize int
	outgoingBufferSize int
	maxMergedChunks    int
	enableMerge        bool
}

func NewShardChunk[T any]() *ShardChunkBuilder[T] {
	return &ShardChunkBuilder[T]{
		ctx:                context.Background(),
		incomingBufferSize: 1000,
		outgoingBufferSize: 100,
		shardCount:         4,
		chunkTimeout:       time.Millisecond * 50,
		chunkSize:          500,
		maxMergedChunks:    1000,
	}
}

func (b *ShardChunkBuilder[T]) WithEnableMerge(enableMerge bool) *ShardChunkBuilder[T] {
	b.enableMerge = enableMerge
	return b
}

func (b *ShardChunkBuilder[T]) WithShardWorker(shardWorker ShardChunkFunc[T]) *ShardChunkBuilder[T] {
	b.shardWorker = shardWorker
	return b
}

func (b *ShardChunkBuilder[T]) WithContext(ctx context.Context) *ShardChunkBuilder[T] {
	b.ctx = ctx
	return b
}

func (b *ShardChunkBuilder[T]) WithChunkSize(chunkSize int) *ShardChunkBuilder[T] {
	b.chunkSize = chunkSize
	return b
}

func (b *ShardChunkBuilder[T]) WithChunkTimeout(chunkTimeout time.Duration) *ShardChunkBuilder[T] {
	b.chunkTimeout = chunkTimeout
	return b
}

func (b *ShardChunkBuilder[T]) WithShardCount(shardCount int) *ShardChunkBuilder[T] {
	b.shardCount = shardCount
	return b
}

func (b *ShardChunkBuilder[T]) WithShardFunc(shardFunc ShardFunc[T]) *ShardChunkBuilder[T] {
	b.shardFunc = shardFunc
	return b
}

func (b *ShardChunkBuilder[T]) WithIncomingBufferSize(incomingBufferSize int) *ShardChunkBuilder[T] {
	b.incomingBufferSize = incomingBufferSize
	return b
}

func (b *ShardChunkBuilder[T]) WithOutgoingBufferSize(outgoingBufferSize int) *ShardChunkBuilder[T] {
	b.outgoingBufferSize = outgoingBufferSize
	return b
}

func (b *ShardChunkBuilder[T]) WithMaxMergedChunks(maxMergedChunks int) *ShardChunkBuilder[T] {
	if maxMergedChunks <= 0 {
		maxMergedChunks = 1
	}
	b.maxMergedChunks = maxMergedChunks
	return b
}

func (b *ShardChunkBuilder[T]) WithIncomingChan(incomingChan chan T) *ShardChunkBuilder[T] {
	b.incomingChan = incomingChan
	return b
}

func (b *ShardChunkBuilder[T]) Build() (*ShardChunk[T], error) {
	if b.incomingChan == nil {
		b.incomingChan = make(chan T, b.incomingBufferSize)
	}
	sharder, err := NewShardChan[T]().WithContext(b.ctx).WithOutgoingBufferSize(b.incomingBufferSize / b.shardCount).WithShardCount(b.shardCount).WithShardFunc(b.shardFunc).WithIncomingChan(b.incomingChan).Build()
	if err != nil {
		return nil, err
	}
	chunkers := make([]*ChunkChan[T], b.shardCount)
	for i := 0; i < b.shardCount; i++ {
		chunkers[i] = NewChunkChan[T]().WithContext(b.ctx).WithChunkSize(b.chunkSize).WithOutgoingBufferSize(b.outgoingBufferSize).WithChunkTimeout(b.chunkTimeout).Build()
		go func(k int) {
			for {
				select {
				case <-b.ctx.Done():
					return
				case m := <-sharder.C(k):
					chunkers[k].Add(m)
				}
			}
		}(i)
	}
	if b.shardWorker != nil {
		for i := 0; i < b.shardCount; i++ {
			if !b.enableMerge {
				go func(k int) {
					for {
						select {
						case <-b.ctx.Done():
							return
						case batch := <-chunkers[k].C():
							b.shardWorker(k, batch)
						}
					}
				}(i)
			} else {
				go func(k int) {
					for {
						select {
						case <-b.ctx.Done():
							return
						case batch := <-chunkers[k].C():
							merged := 1
						drain:
							for len(batch) < b.maxMergedChunks {
								select {
								case more := <-chunkers[k].C():
									batch = append(batch, more...)
									merged++
								default:
									break drain
								}
							}
							//TODO put metrics here
							b.shardWorker(k, batch)
						}
					}
				}(i)
			}
		}
	}

	return &ShardChunk[T]{
		sharder:  sharder,
		chunkers: chunkers,
	}, nil
}
