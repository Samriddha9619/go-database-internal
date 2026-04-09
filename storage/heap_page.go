package storage

import (
	"encoding/binary"
	"mit.edu/dsg/godb/common"
)

// HeapPage Layout:
// LSN (8) | RowSize (2) | NumSlots (2) |  NumUsed (2) | Padding (2) | allocation Bitmap | deleted Bitmap | rows
type HeapPage struct {
	*PageFrame
	allocOffset int
	deletedOffset int
	tupleOffset int
}

func (hp HeapPage) NumUsed() int {
	return int(binary.LittleEndian.Uint16(hp.Bytes[12:14]))
}

func (hp HeapPage) setNumUsed(numUsed int) {
	binary.LittleEndian.PutUint16(hp.Bytes[12:14],uint16(numUsed))
}

func (hp HeapPage) NumSlots() int {
	return int(binary.LittleEndian.Uint16(hp.Bytes[10:12]))
}

func (hp HeapPage) RowSize() int {
	return int(binary.LittleEndian.Uint16(hp.Bytes[8:10]))
}

func InitializeHeapPage(desc *RawTupleDesc, frame *PageFrame) {
	rowSize := desc.BytesPerTuple()

	maxSlots := 0
	for totalSlots := 1; totalSlots <= common.PageSize; totalSlots++ {
		bitmapBytes := ((totalSlots + 63) / 64) * 8
		totalAllocated := 16 + (2 * bitmapBytes) + (totalSlots * rowSize)
		if totalAllocated <= common.PageSize {
			maxSlots = totalSlots
		} else {
			break
		}
	}

	for i := range frame.Bytes { // CPU can make every value 0 even though it looks like it will be manually replacing all 4096 bytes with zeroes.(memclr or memset)
		frame.Bytes[i] = 0
	}

	binary.LittleEndian.PutUint16(frame.Bytes[8:10], uint16(rowSize))
	binary.LittleEndian.PutUint16(frame.Bytes[10:12], uint16(maxSlots))
	binary.LittleEndian.PutUint16(frame.Bytes[12:14], 0)
}

func (frame *PageFrame) AsHeapPage() HeapPage {
	numSlots := int(binary.LittleEndian.Uint16(frame.Bytes[10:12]))

	bitmapSize := ((numSlots + 63) / 64) * 8

	return HeapPage{
		PageFrame:     frame,
		allocOffset:   16,
		deletedOffset: 16 + bitmapSize,
		tupleOffset:   16 + (2 * bitmapSize),
	}
}

func (hp HeapPage) FindFreeSlot() int {
	allocMap := AsBitmap(hp.Bytes[hp.allocOffset:hp.deletedOffset], hp.NumSlots())
	return allocMap.FindFirstZero(0)
}

// IsAllocated checks the allocation bitmap to see if a slot is valid.
func (hp HeapPage) IsAllocated(rid common.RecordID) bool {
    bm := AsBitmap(hp.Bytes[hp.allocOffset:hp.deletedOffset], hp.NumSlots())
    return bm.LoadBit(int(rid.Slot))
}
func (hp HeapPage) MarkAllocated(rid common.RecordID, allocated bool) {
    bm := AsBitmap(hp.Bytes[hp.allocOffset:hp.deletedOffset], hp.NumSlots())
    wasAllocated := bm.SetBit(int(rid.Slot), allocated)
    
    if allocated && !wasAllocated {
        hp.setNumUsed(hp.NumUsed() + 1)
    } else if !allocated && wasAllocated {
        hp.setNumUsed(hp.NumUsed() - 1)
    }
    
    if !allocated {
        hp.MarkDeleted(rid, false)
    } else {
        hp.MarkDeleted(rid, false)
    }
}
func (hp HeapPage) IsDeleted(rid common.RecordID) bool {
    // deletedOffset to tupleOffset is the boundary for the deleted bitmap
    bm := AsBitmap(hp.Bytes[hp.deletedOffset:hp.tupleOffset], hp.NumSlots())
    return bm.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkDeleted(rid common.RecordID, deleted bool) {
    bm := AsBitmap(hp.Bytes[hp.deletedOffset:hp.tupleOffset], hp.NumSlots())
    bm.SetBit(int(rid.Slot), deleted)
}

func (hp HeapPage) AccessTuple(rid common.RecordID) RawTuple {
    start := hp.tupleOffset + (int(rid.Slot) * hp.RowSize())
    end := start + hp.RowSize()
    return hp.Bytes[start:end]
}